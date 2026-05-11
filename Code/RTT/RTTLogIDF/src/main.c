/*
 * FTM + CSI Collector – XIAO ESP32-S3
 * Phoenix SAR – NLOS/LOS Dataset Collection Firmware  v3
 *
 * REQUIREMENTS – read before flashing:
 * ─────────────────────────────────────────────────────────────────
 * 1. Place sdkconfig.defaults in the PROJECT ROOT (next to platformio.ini).
 *    It must contain:  CONFIG_ESP_WIFI_CSI_ENABLED=y
 *    Without this Kconfig flag the CSI callback is never fired, even though
 *    esp_wifi_set_csi_config() returns ESP_OK.
 *
 * 2. After adding sdkconfig.defaults:
 *      pio run -t clean   (wipe the generated sdkconfig)
 *      pio run -t upload  (full rebuild)
 *
 * 3. platformio.ini must use framework = espidf, not arduino.
 *    FTM is not available in the Arduino Wi-Fi wrapper.
 *
 * Serial output lines:
 *   BURST_START,<seq>,<n_frames>,<label>
 *   FTM_F,<seq>,<frame_idx>,<rtt_ps>,<t1_ps>,<t2_ps>,<t3_ps>,<t4_ps>,<rssi_dbm>,<label>
 *   CSI,<seq>,<rssi_dbm>,<noise_floor_dbm>,<n_sub>,<amp0>,...,<ampN-1>,<label>
 *   LABEL,<label>   (echo when label changed via serial)
 *
 * Label commands via serial (115200 baud) – send string + newline:
 *   LOS_STATIC  LOS_DYNAMIC  NLOS_WALL  NLOS_CORNER  NLOS_DOOR  NLOS_DYNAMIC
 *
 * ESP32-S3 CSI hardware notes:
 *   • first_word_invalid flag: on S3 the first 4 bytes of the CSI buf can be
 *     corrupted. We skip subcarrier 0 (bytes 0-1) always to be safe.
 *   • Buffer layout: [I0,Q0, I1,Q1, … I63,Q63] as signed int8.
 *   • Useful subcarriers for HT20: indices 1-26 and 38-63 (skip DC at 0,
 *     and guard bands). We use a contiguous window 6-57 (52 subs) which
 *     avoids both the invalid first word and the outer guards.
 *   • CSI is generated on every received Wi-Fi frame (beacons, data, FTM
 *     action frames). You will see CSI even before FTM starts.
 */

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <inttypes.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "freertos/queue.h"
#include "nvs_flash.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"

/* ── Configuration ────────────────────────────────────────────────────────── */
#define WIFI_SSID            "FTM_AP"
#define WIFI_PASS            "12345678"
#define WIFI_CHANNEL         6

#define FTM_FRMS_PER_BURST   64
#define FTM_BURST_PERIOD     0
#define MEASURE_INTERVAL_MS  600
#define MAX_RETRY            5
#define MAX_FRAME_ENTRIES    64

/*
 * CSI subcarrier window.
 * Skip index 0 (first_word_invalid risk on S3) and outer guard bands.
 * Indices 6-57 give 52 clean subcarriers for HT20.
 */
#define CSI_TOTAL_SUB        64     /* total I/Q pairs in driver buffer       */
#define CSI_BUF_BYTES        128    /* 64 pairs × 2 bytes each                */
#define CSI_SUB_START         6     /* first subcarrier index we keep          */
#define CSI_SUB_END          57     /* last  subcarrier index we keep          */
#define CSI_N_SUB            (CSI_SUB_END - CSI_SUB_START + 1)  /* 52        */

#define CMD_BUF_LEN          32

/* ── Event bits ───────────────────────────────────────────────────────────── */
#define WIFI_CONNECTED_BIT   BIT0
#define WIFI_FAIL_BIT        BIT1
#define FTM_DONE_BIT         BIT2

/* ── Globals ──────────────────────────────────────────────────────────────── */
static EventGroupHandle_t s_wifi_evt_group;
static EventGroupHandle_t s_ftm_evt_group;

static const char *TAG        = "PHOENIX";
static int         s_retry    = 0;
static uint32_t    s_seq      = 0;
static uint8_t     s_ap_bssid[6] = {0};
static uint8_t     s_ap_ch       = 0;
static bool        s_csi_ok      = false;

static char s_label[CMD_BUF_LEN] = "LOS_STATIC";

/* ── FTM snapshot ─────────────────────────────────────────────────────────── */
typedef struct {
    wifi_ftm_status_t       status;
    uint8_t                 n;
    wifi_ftm_report_entry_t e[MAX_FRAME_ENTRIES];
} ftm_snap_t;
static ftm_snap_t s_snap;

/* ── CSI record ───────────────────────────────────────────────────────────── */
typedef struct {
    uint32_t seq;
    int8_t   rssi;
    int8_t   noise;
    uint16_t amp[CSI_N_SUB];   /* amplitude × 10 fixed-point, uint16 */
} csi_rec_t;

#define CSI_Q_DEPTH 16
static QueueHandle_t s_csi_q = NULL;

/* ── CSI callback ─────────────────────────────────────────────────────────── */
static void IRAM_ATTR csi_cb(void *ctx, wifi_csi_info_t *info)
{
    if (!info || !info->buf) return;

    /*
     * info->len is the number of BYTES in the buffer.
     * We need at least CSI_BUF_BYTES to index subcarrier CSI_SUB_END.
     * If the packet is shorter (e.g. legacy LLTF only), still process
     * whatever is available.
     */
    int available_subs = info->len / 2;   /* each sub = 2 bytes (I,Q) */
    if (available_subs < CSI_SUB_START + 1) return;   /* nothing useful */

    int end = (available_subs - 1 < CSI_SUB_END) ? available_subs - 1
                                                   : CSI_SUB_END;
    int n_out = end - CSI_SUB_START + 1;
    if (n_out <= 0) return;

    csi_rec_t rec;
    rec.seq   = s_seq;
    rec.rssi  = info->rx_ctrl.rssi;
    rec.noise = info->rx_ctrl.noise_floor;

    /* Zero-fill output so the struct is always CSI_N_SUB wide */
    memset(rec.amp, 0, sizeof(rec.amp));

    const int8_t *buf = (const int8_t *)info->buf;

    for (int i = 0; i < n_out; i++) {
        int sub = CSI_SUB_START + i;
        /*
         * S3 first_word_invalid: if set, bytes [0..3] (subs 0 and 1) are
         * corrupted. We start at sub 6 so we're already safe, but guard
         * explicitly anyway.
         */
        if (info->first_word_invalid && sub < 2) {
            rec.amp[i] = 0;
            continue;
        }
        float I   = (float)buf[sub * 2];       /* imaginary (index 0 = imag) */
        float Q   = (float)buf[sub * 2 + 1];   /* real      (index 1 = real)  */
        float amp = sqrtf(I * I + Q * Q);
        uint32_t scaled = (uint32_t)(amp * 10.0f + 0.5f);
        rec.amp[i] = (uint16_t)(scaled > 65535u ? 65535u : scaled);
    }

    /* Non-blocking – drop record if queue full rather than block Wi-Fi task */
    xQueueSendFromISR(s_csi_q, &rec, NULL);
}

/* ── CSI enable – called only from IP_GOT_IP handler ─────────────────────── */
static void csi_enable(void)
{
    if (s_csi_ok) return;

    /*
     * wifi_csi_config_t field names differ between IDF versions.
     * Use designated initialisers with only the fields present in IDF ≥ 5.0.
     * Leaving unspecified fields as 0/false is safe.
     */
    wifi_csi_config_t cfg = {
        .lltf_en           = true,
        .htltf_en          = true,
        .stbc_htltf2_en    = false,   /* set false – reduces noise on S3    */
        .ltf_merge_en      = true,
        .channel_filter_en = false,   /* keep raw sub-carrier data           */
        .manu_scale        = false,
    };

    esp_err_t r;

    r = esp_wifi_set_csi_config(&cfg);
    if (r != ESP_OK) {
        ESP_LOGE(TAG, "set_csi_config: %s", esp_err_to_name(r));
        ESP_LOGE(TAG, ">>> Is CONFIG_ESP_WIFI_CSI_ENABLED=y in sdkconfig.defaults? <<<");
        return;
    }

    r = esp_wifi_set_csi_rx_cb(csi_cb, NULL);
    if (r != ESP_OK) {
        ESP_LOGE(TAG, "set_csi_rx_cb: %s", esp_err_to_name(r));
        return;
    }

    r = esp_wifi_set_csi(true);
    if (r != ESP_OK) {
        ESP_LOGE(TAG, "set_csi(true): %s", esp_err_to_name(r));
        return;
    }

    s_csi_ok = true;
    ESP_LOGI(TAG, "CSI enabled  subs=[%d..%d] (%d total)", CSI_SUB_START, CSI_SUB_END, CSI_N_SUB);
}

/* ── Wi-Fi event handler ─────────────────────────────────────────────────── */
static void evt_handler(void *arg, esp_event_base_t base,
                         int32_t id, void *data)
{
    if (base == WIFI_EVENT) {
        switch (id) {
            case WIFI_EVENT_STA_START:
                esp_wifi_connect();
                break;

            case WIFI_EVENT_STA_DISCONNECTED:
                s_csi_ok = false;
                if (s_retry < MAX_RETRY) {
                    esp_wifi_connect();
                    ESP_LOGW(TAG, "Retry %d/%d", ++s_retry, MAX_RETRY);
                } else {
                    xEventGroupSetBits(s_wifi_evt_group, WIFI_FAIL_BIT);
                }
                break;

            case WIFI_EVENT_FTM_REPORT: {
                wifi_event_ftm_report_t *rep = (wifi_event_ftm_report_t *)data;
                s_snap.status = rep->status;
                s_snap.n      = 0;
                if (rep->ftm_report_num_entries > 0 && rep->ftm_report_data) {
                    uint8_t n = (uint8_t)rep->ftm_report_num_entries;
                    if (n > MAX_FRAME_ENTRIES) n = MAX_FRAME_ENTRIES;
                    memcpy(s_snap.e, rep->ftm_report_data,
                           n * sizeof(wifi_ftm_report_entry_t));
                    s_snap.n = n;
                }
                xEventGroupSetBits(s_ftm_evt_group, FTM_DONE_BIT);
                break;
            }
            default: break;
        }

    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        s_retry = 0;
        wifi_ap_record_t ap;
        if (esp_wifi_sta_get_ap_info(&ap) == ESP_OK) {
            memcpy(s_ap_bssid, ap.bssid, 6);
            s_ap_ch = ap.primary;
            ESP_LOGI(TAG, "AP %02x:%02x:%02x:%02x:%02x:%02x ch=%d",
                     s_ap_bssid[0], s_ap_bssid[1], s_ap_bssid[2],
                     s_ap_bssid[3], s_ap_bssid[4], s_ap_bssid[5], s_ap_ch);
        }
        csi_enable();   /* safe to call here – STA is fully associated */
        xEventGroupSetBits(s_wifi_evt_group, WIFI_CONNECTED_BIT);
    }
}

/* ── Wi-Fi init ───────────────────────────────────────────────────────────── */
static bool wifi_init_sta(void)
{
    s_wifi_evt_group = xEventGroupCreate();
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        WIFI_EVENT, ESP_EVENT_ANY_ID, evt_handler, NULL, NULL));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        IP_EVENT, IP_EVENT_STA_GOT_IP, evt_handler, NULL, NULL));

    wifi_config_t wc = {
        .sta = {
            .ssid        = WIFI_SSID,
            .password    = WIFI_PASS,
            .channel     = WIFI_CHANNEL,
            .scan_method = WIFI_FAST_SCAN,
        },
    };
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wc));
    ESP_ERROR_CHECK(esp_wifi_start());
    ESP_LOGI(TAG, "Connecting to %s ...", WIFI_SSID);

    EventBits_t b = xEventGroupWaitBits(s_wifi_evt_group,
                                         WIFI_CONNECTED_BIT | WIFI_FAIL_BIT,
                                         pdFALSE, pdFALSE,
                                         pdMS_TO_TICKS(15000));
    if (b & WIFI_CONNECTED_BIT) { ESP_LOGI(TAG, "Connected."); return true; }
    ESP_LOGE(TAG, "Connection failed.");
    return false;
}

/* ── Serial command task ──────────────────────────────────────────────────── */
static void serial_cmd_task(void *pv)
{
    char buf[CMD_BUF_LEN];
    int  pos = 0;
    while (true) {
        int c = fgetc(stdin);
        if (c == EOF) { vTaskDelay(pdMS_TO_TICKS(10)); continue; }
        if (c == '\n' || c == '\r') {
            if (pos > 0) {
                buf[pos] = '\0';
                if (strcmp(buf, "LOS_STATIC")   == 0 ||
                    strcmp(buf, "LOS_DYNAMIC")  == 0 ||
                    strcmp(buf, "NLOS_WALL")    == 0 ||
                    strcmp(buf, "NLOS_CORNER")  == 0 ||
                    strcmp(buf, "NLOS_DOOR")    == 0 ||
                    strcmp(buf, "NLOS_DYNAMIC") == 0)
                {
                    strncpy(s_label, buf, CMD_BUF_LEN - 1);
                    s_label[CMD_BUF_LEN - 1] = '\0';
                    printf("LABEL,%s\n", s_label);
                    fflush(stdout);
                } else {
                    printf("# Unknown: %s\n", buf);
                    fflush(stdout);
                }
                pos = 0;
            }
        } else if (pos < CMD_BUF_LEN - 1) {
            buf[pos++] = (char)c;
        }
    }
}

/* ── FTM + CSI measure task ───────────────────────────────────────────────── */
static void ftm_task(void *pv)
{
    s_ftm_evt_group = xEventGroupCreate();

    printf("# SCHEMA FTM_F: seq,frame_idx,rtt_ps,t1_ps,t2_ps,t3_ps,t4_ps,rssi_dbm,label\n");
    printf("# SCHEMA CSI:   seq,rssi_dbm,noise_dbm,n_sub,amp[0..51]×0.1,label\n");
    printf("# CSI_N_SUB=%d  (subcarriers %d-%d)\n", CSI_N_SUB, CSI_SUB_START, CSI_SUB_END);
    fflush(stdout);

    while (true) {
        /* Snapshot label */
        char lbl[CMD_BUF_LEN];
        strncpy(lbl, s_label, CMD_BUF_LEN - 1);
        lbl[CMD_BUF_LEN - 1] = '\0';

        /* Drain stale CSI */
        { csi_rec_t tmp; while (xQueueReceive(s_csi_q, &tmp, 0) == pdTRUE) {} }

        /* Initiate FTM */
        wifi_ftm_initiator_cfg_t fc = {
            .resp_mac           = {0},
            .channel            = s_ap_ch,
            .frm_count          = FTM_FRMS_PER_BURST,
            .burst_period       = FTM_BURST_PERIOD,
            .use_get_report_api = false,
        };
        memcpy(fc.resp_mac, s_ap_bssid, 6);

        esp_err_t err = esp_wifi_ftm_initiate_session(&fc);
        if (err != ESP_OK) {
            ESP_LOGE(TAG, "FTM init: %s", esp_err_to_name(err));
            vTaskDelay(pdMS_TO_TICKS(MEASURE_INTERVAL_MS));
            continue;
        }

        EventBits_t b = xEventGroupWaitBits(s_ftm_evt_group, FTM_DONE_BIT,
                                             pdTRUE, pdFALSE,
                                             pdMS_TO_TICKS(5000));
        if (!(b & FTM_DONE_BIT)) {
            ESP_LOGW(TAG, "FTM timeout");
            esp_wifi_ftm_end_session();
            vTaskDelay(pdMS_TO_TICKS(MEASURE_INTERVAL_MS));
            continue;
        }
        if (s_snap.status != FTM_STATUS_SUCCESS) {
            ESP_LOGW(TAG, "FTM status=%d", (int)s_snap.status);
            vTaskDelay(pdMS_TO_TICKS(MEASURE_INTERVAL_MS));
            continue;
        }

        /* BURST_START */
        printf("BURST_START,%" PRIu32 ",%u,%s\n", s_seq, s_snap.n, lbl);

        /* FTM frames */
        for (uint8_t i = 0; i < s_snap.n; i++) {
            wifi_ftm_report_entry_t *e = &s_snap.e[i];
            printf("FTM_F,%" PRIu32 ",%u,%" PRIu32
                   ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%d,%s\n",
                   s_seq, (unsigned)i, e->rtt,
                   e->t1, e->t2, e->t3, e->t4,
                   (int)e->rssi, lbl);
        }

        /* CSI packets collected during this burst */
        csi_rec_t cr;
        int csi_count = 0;
        while (xQueueReceive(s_csi_q, &cr, 0) == pdTRUE) {
            printf("CSI,%" PRIu32 ",%d,%d,%d",
                   s_seq, (int)cr.rssi, (int)cr.noise, CSI_N_SUB);
            for (int i = 0; i < CSI_N_SUB; i++) {
                printf(",%u", (unsigned)cr.amp[i]);
            }
            printf(",%s\n", lbl);
            csi_count++;
        }

        if (csi_count == 0 && s_csi_ok) {
            /* CSI is enabled but no packets arrived during this burst.
             * This can happen if no Wi-Fi frames were received. Print a
             * diagnostic so the Python logger can flag sparse CSI. */
            printf("# CSI_EMPTY seq=%" PRIu32 "\n", s_seq);
        }

        fflush(stdout);
        s_seq++;

        vTaskDelay(pdMS_TO_TICKS(MEASURE_INTERVAL_MS));
    }
}

/* ── app_main ─────────────────────────────────────────────────────────────── */
void app_main(void)
{
    esp_err_t r = nvs_flash_init();
    if (r == ESP_ERR_NVS_NO_FREE_PAGES || r == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        r = nvs_flash_init();
    }
    ESP_ERROR_CHECK(r);
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    s_csi_q = xQueueCreate(CSI_Q_DEPTH, sizeof(csi_rec_t));
    configASSERT(s_csi_q);

    if (!wifi_init_sta()) {
        ESP_LOGE(TAG, "Halting.");
        return;
    }

    xTaskCreate(serial_cmd_task, "serial_cmd", 3072, NULL, 3, NULL);
    xTaskCreate(ftm_task,        "ftm",        6144, NULL, 5, NULL);
}