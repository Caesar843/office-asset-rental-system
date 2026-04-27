# Competition Runbook

This runbook is the final rehearsal and competition-day execution guide for Step 10.
It keeps two paths ready:

- Real serial device path: primary demo plan.
- Mock MCU socket path: fallback demo plan.

## 1. Preflight

### 1.1 MySQL

Verify the live repository configuration first:

```powershell
python C:\Users\lenovo\office-asset-rental-system\backend_service\mysql_runtime_check.py
```

Expected result:

- `repository_mode == "mysql"`
- `repository_ready == true`
- `repository_status == "ok"` or `repository_status == "warning"`

### 1.2 Real serial device

Point `BACKEND_SERIAL_PORT` at a physical device path, not `socket://...`.

Windows example:

```powershell
$env:BACKEND_SERIAL_PORT="COM3"
```

Linux example:

```bash
export BACKEND_SERIAL_PORT=/dev/ttyUSB0
```

Run the real-device preflight:

```powershell
python C:\Users\lenovo\office-asset-rental-system\backend_service\check_real_serial_runtime.py --serial-port COM3
```

Expected result:

- `mode == "real_serial"`
- `serial_open == true`
- `device_status == "ONLINE"`
- `ready_for_real_demo == true`

## 2. Real Device Demo Plan

### 2.1 Required environment variables

```powershell
$env:BACKEND_REPOSITORY_KIND="mysql"
$env:BACKEND_SERIAL_PORT="COM3"
$env:BACKEND_SERIAL_BAUDRATE="115200"
$env:BACKEND_SERIAL_ACK_TIMEOUT="0.5"
$env:BACKEND_SERIAL_MAX_RETRIES="3"
$env:BACKEND_SERIAL_OFFLINE_TIMEOUT="15.0"
```

Keep the existing MySQL environment variables from Step 8:

- `BACKEND_DB_HOST`
- `BACKEND_DB_PORT`
- `BACKEND_DB_NAME`
- `BACKEND_DB_USER`
- `BACKEND_DB_PASSWORD`

### 2.2 Startup order

1. Run `mysql_runtime_check.py`
2. Run `check_real_serial_runtime.py`
3. Start the API

```powershell
python C:\Users\lenovo\office-asset-rental-system\backend_service\api_app.py
```

4. Open `/health`

```text
http://127.0.0.1:8000/health
```

### 2.3 What to check in `/health`

Check repository first:

- `repository_mode`
- `repository_status`

Then check serial/device state:

- `serial_details.mode`
- `serial_details.configured_port`
- `serial_details.diagnosis`
- `device_status`
- `serial_open`

Real-device mode must show:

- `serial_details.mode == "real_serial"`
- `serial_details.is_real_serial_mode == true`
- `configured_port` is `COMx` or `/dev/ttyUSBx`
- it must not start with `socket://`

### 2.4 Official real-device one-key rehearsals

Borrow:

```powershell
python C:\Users\lenovo\office-asset-rental-system\backend_service\run_real_device_flow.py --action borrow --asset-id AS-0924 --user-id U-1001 --user-name "Demo Borrow" --repository-kind mysql --serial-port COM3 --timeout-ms 30000
```

Return:

```powershell
python C:\Users\lenovo\office-asset-rental-system\backend_service\run_real_device_flow.py --action return --asset-id AS-0925 --user-id U-1002 --user-name "Demo Return" --repository-kind mysql --serial-port COM3 --timeout-ms 30000
```

Inbound:

```powershell
python C:\Users\lenovo\office-asset-rental-system\backend_service\run_real_device_flow.py --action inbound --asset-id AS-0926 --user-id U-ADMIN --user-name "Demo Admin" --asset-name "Demo Inbound Asset" --category-id 1 --location "Inbound Shelf" --repository-kind mysql --serial-port COM3 --timeout-ms 30000
```

The script output must show:

- `action == "inbound"`
- `request_target == "/transactions/inbound"`
- the JSON request body with `asset_id / user_id / user_name / asset_name / category_id / location / timeout_ms`
- `api_response_body`
- `websocket_messages`

### 2.5 Borrow demo

```powershell
curl.exe -X POST http://127.0.0.1:8000/transactions/borrow `
  -H "Content-Type: application/json" `
  -d "{\"asset_id\":\"AS-0924\",\"user_id\":\"U-1001\",\"user_name\":\"Demo Borrow\",\"timeout_ms\":30000}"
```

Observe:

- API result reaches the final business result
- the real device performs the physical confirmation step
- WebSocket shows waiting states and the final state

### 2.6 Return demo

```powershell
curl.exe -X POST http://127.0.0.1:8000/transactions/return `
  -H "Content-Type: application/json" `
  -d "{\"asset_id\":\"AS-0925\",\"user_id\":\"U-1002\",\"user_name\":\"Demo Return\",\"timeout_ms\":30000}"
```

### 2.7 Inbound demo

```powershell
curl.exe -X POST http://127.0.0.1:8000/transactions/inbound `
  -H "Content-Type: application/json" `
  -d "{\"asset_id\":\"AS-0926\",\"user_id\":\"U-ADMIN\",\"user_name\":\"Demo Admin\",\"asset_name\":\"Demo Inbound Asset\",\"category_id\":1,\"location\":\"Inbound Shelf\",\"timeout_ms\":30000}"
```

Minimum success criteria:

- API returns `success == true`
- API returns `code == "CONFIRMED"`
- `action_type == "INBOUND"`
- `transaction_state == "COMPLETED"`
- WebSocket shows `WAITING_ACK`, `WAITING_HW`, then the final success result
- `assets` contains the new row with `qr_code == "AS-0926"` and `status == IN_STOCK`
- `operation_records` contains one `op_type == "INBOUND"` row for the same asset, with `request_seq / request_id / hw_seq / hw_result / hw_sn`

### 2.8 WebSocket observation

Connect:

```text
ws://127.0.0.1:8000/ws/status
```

At minimum, observe:

- `WAITING_ACK`
- `WAITING_HW`
- `CONFIRMED` or the failure code

For inbound rehearsals, also confirm the final WebSocket payload references:

- `action_type == "INBOUND"`
- the same `asset_id`
- the same `request_seq` / `request_id`

### 2.9 DataGrip / MySQL observation

Focus on these tables:

- `assets`
- `operation_records`

Focus on these fields:

- `assets.status`
- `operation_records.op_type`
- `operation_records.op_time`
- `operation_records.hw_seq`
- `operation_records.hw_result`

For inbound, also check:

- `assets.qr_code`
- `assets.asset_name`
- `assets.location`
- `operation_records.request_seq`
- `operation_records.request_id`
- `operation_records.hw_sn`

## 3. Real Device Troubleshooting

### 3.1 `/health` is degraded and the serial side failed

Check:

- `serial_details.startup_error_kind`
- `serial_details.startup_error`
- `serial_details.next_steps`

Common cases:

- `pyserial_missing`
- `serial_port_not_found`
- `serial_port_busy`
- `waiting_for_heartbeat`
- `device_offline`

### 3.2 `ACK_TIMEOUT`

Check in order:

1. Is the device online?
2. Is the API connected to the correct physical serial port?
3. Is the device powered and running the expected firmware?
4. Are the cable and driver stable?

### 3.3 The serial port opens but the device stays `UNKNOWN`

This means:

- the port itself opened
- but the API still did not receive heartbeat frames

Check:

1. Is the MCU sending heartbeat?
2. Is the baudrate correct?
3. Is the device still attached to the same serial port?

## 4. Mock Fallback Plan

### 4.1 Fastest way to switch back to mock

```powershell
python C:\Users\lenovo\office-asset-rental-system\backend_service\start_demo_stack.py --transport-mode mock --repository-kind mysql --mock-mode confirmed
```

Or start the two parts separately:

```powershell
python C:\Users\lenovo\office-asset-rental-system\serial_comm\mock_mcu.py --host 127.0.0.1 --port 9100 --mode confirmed
$env:BACKEND_REPOSITORY_KIND="mysql"
$env:BACKEND_SERIAL_PORT="socket://127.0.0.1:9100"
python C:\Users\lenovo\office-asset-rental-system\backend_service\api_app.py
```

### 4.2 Validate the mock path

Borrow:

```powershell
python C:\Users\lenovo\office-asset-rental-system\backend_service\run_mock_api_flow.py --action borrow --asset-id AS-0924 --user-id U-1001 --user-name Demo --repository-kind mysql --mock-mode confirmed
```

Inbound:

```powershell
python C:\Users\lenovo\office-asset-rental-system\backend_service\run_mock_api_flow.py --action inbound --asset-id AS-0926 --user-id U-ADMIN --user-name "Demo Admin" --asset-name "Demo Inbound Asset" --category-id 1 --location "Inbound Shelf" --repository-kind mysql --mock-mode confirmed --timeout-ms 3000
```

Minimum mock inbound success criteria:

- script output shows `request_target == "/transactions/inbound"`
- script output shows the inbound request body with `asset_name / category_id / location`
- `api_response_body.success == true`
- `api_response_body.code == "CONFIRMED"`
- `websocket_messages` contains waiting states plus the final success result
- `asset_snapshot_after` shows the new inbound asset
- `repository_asset_status_after` is the in-stock value

### 4.3 Suggested judge-facing explanation

State the difference clearly:

- The primary plan is the real serial device confirmation path.
- The fallback plan temporarily switches only the hardware input source to mock.
- API, transaction state machine, WebSocket, repository and MySQL still stay on the same backend path.

## 5. Switching Rules

### 5.1 Real -> Mock

Change:

- `BACKEND_SERIAL_PORT=COMx`

To:

- `BACKEND_SERIAL_PORT=socket://127.0.0.1:9100`

Then start `mock_mcu.py`.

### 5.2 Mock -> Real

Change:

- `BACKEND_SERIAL_PORT=socket://127.0.0.1:9100`

Back to:

- `BACKEND_SERIAL_PORT=COMx` or `/dev/ttyUSBx`

Then always rerun:

```powershell
python C:\Users\lenovo\office-asset-rental-system\backend_service\check_real_serial_runtime.py --serial-port COM3
```

## 6. Final Rehearsal Order

Run these in order before the competition:

1. `mysql_runtime_check.py`
2. `check_real_serial_runtime.py`
3. `run_real_device_flow.py --action borrow`
4. `run_real_device_flow.py --action return`
5. `run_real_device_flow.py --action inbound`
6. If the real device fails, run `start_demo_stack.py --transport-mode mock`
7. In mock fallback mode, run `run_mock_api_flow.py --action inbound`
