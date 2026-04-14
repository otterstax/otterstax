# S3 и File Storage — Design Document

## Обзор

Этот документ описывает архитектуру и план реализации поддержки S3-хранилища и локальных файлов (JSON, CSV, Parquet) в OtterStax. Реализация следует существующим паттернам ClickHouse backend — то же разделение на `connectors/`, `db_integration/`, `otterbrix/translators/`.

---

## 1. Архитектура системы (как устроены существующие backends)

Каждый backend в OtterStax реализуется через четыре слоя:

```
┌─────────────────────────────────────────────────────┐
│                    Scheduler                        │
│  (routes/scheduler.hpp — enum route)                │
└───────────┬─────────────────────────────────────────┘
            │ actor message
            ▼
┌─────────────────────────────────────────────────────┐
│           db_integration/<backend>/                 │
│           ConnectionManager (actor)                 │
│  Получает ParsedQueryDataPtr, запускает выборку,    │
│  конвертирует результат, отправляет finish-message  │
└───────────┬─────────────────────────────────────────┘
            │ calls
            ▼
┌─────────────────────────────────────────────────────┐
│           connectors/<backend>/                     │
│           ConnectorManager + IConnector             │
│  Пул соединений, async выполнение через ASIO,       │
│  возвращает future<T> (T зависит от handler)        │
└───────────┬─────────────────────────────────────────┘
            │ converts
            ▼
┌─────────────────────────────────────────────────────┐
│     otterbrix/translators/input/<backend>_to_chunk  │
│  Backend-specific результат → data_chunk_t          │
└─────────────────────────────────────────────────────┘
```

Аналогичную структуру нужно повторить для `file` и `s3` backends.

---

## 2. Что нужно добавить (полный список файлов)

### 2.1 Новые файлы

```
connectors/file/
├── types.hpp                    # Status enum, connect_params (path, format)
├── connector.hpp                # IConnector + FileConnector
├── connector.cpp
├── manager.hpp                  # ConnectorManager (пул соединений)
└── manager.cpp

connectors/s3/
├── types.hpp                    # Status enum, connect_params (bucket, region, credentials)
├── connector.hpp                # s3c::IConnector + S3Connector
├── connector.cpp
├── manager.hpp
└── manager.cpp

db_integration/file/
├── connection_manager.hpp       # FileConnectionManager (actor)
└── connection_manager.cpp

db_integration/s3/
├── connection_manager.hpp       # S3ConnectionManager (actor)
└── connection_manager.cpp

otterbrix/translators/input/
├── arrow_to_chunk.hpp           # arrow::RecordBatch → data_chunk_t (ОБЩИЙ для всех форматов)
├── arrow_to_chunk.cpp
├── json_to_chunk.hpp            # JSON  → arrow::Table → data_chunk_t
├── json_to_chunk.cpp
├── csv_to_chunk.hpp             # CSV   → arrow::Table → data_chunk_t
├── csv_to_chunk.cpp
├── parquet_to_chunk.hpp         # Parquet → arrow::Table → data_chunk_t
└── parquet_to_chunk.cpp

routes/
├── file_connection_manager.hpp  # enum route { execute }
└── s3_connection_manager.hpp    # enum route { execute }
```

### 2.2 Изменения в существующих файлах

| Файл | Изменение |
|------|-----------|
| `otterbrix/parser/parser.hpp` | Добавить `File = 6`, `S3 = 7` в `backend_type_t` |
| `routes/scheduler.hpp` | Добавить два новых `route` значения |
| `catalog/catalog_manager.hpp` | Добавить `File`, `S3` в `ConnectionType`; зарегистрировать новые managers |
| `component_manager/component_manager.hpp` | Объявить новые managers |
| `component_manager/component_manager.cpp` | Создать и подключить новые managers в конструкторе |
| `otterbrix/query_generation/sql_query_generator.hpp` | Добавить case для `backend_type_t::File` / `backend_type_t::S3` |
| `connectors/http_server/connection_server.hpp/cpp` | Расширить конструктор `Server(io_context&, port, mysql_mgr, pg_mgr, ch_mgr)` двумя новыми параметрами: `shared_ptr<filec::ConnectorManager>`, `shared_ptr<s3c::ConnectorManager>`; добавить эндпоинты `/add_file_connection`, `/add_s3_connection`, `/check_file_connection`, `/check_s3_connection` |
| `conanfile.py` | Добавить Arrow опции: `with_parquet=True`, `with_csv=True`, `with_json=True`; добавить `aws-sdk-cpp` для S3 |
| `CMakeLists.txt` | Добавить `find_package(Parquet REQUIRED)`, линковку новых модулей |

---

## 3. Детальный дизайн слоёв

### 3.1 `connectors/file/types.hpp`

```cpp
namespace filec {

// Повторяет паттерн chc::Status / pgc::Status / mysqlc::Status:
enum class Status : uint8_t { Created, Connected, Disconnected, Working, Closed };

enum class FileFormat : uint8_t { JSON, CSV, Parquet, Auto };

struct connect_params {
    std::string path;            // Путь к файлу или директории
    FileFormat  format;          // Формат файла (или Auto — определить по расширению)
    char        csv_delimiter;   // Разделитель для CSV (по умолчанию ',')
    bool        csv_header;      // Есть ли заголовок в CSV (по умолчанию true)
    std::string alias;           // Логическое имя (используется как table name)
};

} // namespace filec
```

### 3.2 `connectors/file/connector.hpp`

Существующие backends (MySQL, PostgreSQL, ClickHouse) реализуют **три перегрузки** `runQuery()` с разными типами возврата:
- `asio::awaitable<std::unique_ptr<data_chunk_t>>` — для SELECT-запросов (данные)
- `asio::awaitable<int64_t>` — для DML-запросов (affected rows)
- `asio::awaitable<components::catalog::catalog_error>` — для DDL/ошибок

Каждая перегрузка принимает handler с backend-специфичным типом аргумента (например, у ClickHouse это `const std::vector<clickhouse::Block>&`).

Для файлового коннектора промежуточный тип данных — `FileData` (обёртка над прочитанными байтами):

```cpp
namespace filec {

// Промежуточный тип — содержимое файла до парсинга в data_chunk_t
struct FileData {
    std::vector<uint8_t> bytes;
    FileFormat           format;
    std::string          file_path;   // для диагностики
};

class IConnector {
public:
    virtual ~IConnector() = default;
    virtual Status status() const noexcept = 0;
    virtual const connect_params& params() const noexcept = 0;
    virtual void connect() = 0;
    virtual bool isConnected() noexcept = 0;
    virtual void close() noexcept = 0;
    virtual bool isClosed() const noexcept = 0;
    virtual std::string alias() const noexcept = 0;
    virtual void tryReconnect() = 0;

    // Три перегрузки — повторяют паттерн chc::IConnector / mysqlc::IConnector:
    // 1. SELECT — возвращает data_chunk_t через handler
    virtual asio::awaitable<std::unique_ptr<components::vector::data_chunk_t>>
    runQuery(std::string_view query,
             std::function<std::unique_ptr<components::vector::data_chunk_t>(const FileData&)> handler) = 0;

    // 2. DML (не применимо к файлам, но нужно для единообразия интерфейса)
    virtual asio::awaitable<int64_t>
    runQuery(std::string_view query,
             std::function<int64_t(const FileData&)> handler) = 0;

    // 3. DDL/schema (например, определение схемы файла)
    virtual asio::awaitable<components::catalog::catalog_error>
    runQuery(std::string_view query,
             std::function<components::catalog::catalog_error(const FileData&)> handler) = 0;
};

class FileConnector : public IConnector {
    connect_params params_;
    Status         status_;
    std::pmr::memory_resource* resource_;

    // Читает файл и заполняет FileData
    FileData read_file();
public:
    FileConnector(connect_params params, std::pmr::memory_resource* res);
    // ... реализует все 3 перегрузки runQuery()
};

} // namespace filec
```

**Примечание:** `runQuery()` у файлового коннектора — это чтение всего файла (или части при наличии partition pruning). SQL-фильтрация выполняется уже внутри Otterbrix после передачи `data_chunk_t`. Параметр `query` для файлов фактически игнорируется — данные всегда читаются целиком.

### 3.3 `connectors/file/manager.hpp`

Повторяет паттерн `chc::ConnectorManager`:

```cpp
namespace filec {

// Фабрика коннекторов (по аналогии с chc::connector_factory)
using connector_factory = std::function<std::unique_ptr<IConnector>(connect_params, std::string)>;
std::unique_ptr<IConnector> make_file_connector(connect_params params, std::string alias);

class ConnectorManager {
    // Повторяет private-members chc::ConnectorManager (connectors/clickhouse/manager.hpp:82-87):
    log_t                      log_;
    thread_pool_manager        thread_pool_manager_;  // utility/thread_pool_manager.hpp — wraps asio::io_context + jthreads
    actor_zeta::address_t      catalog_manager_;
    connector_factory          make_connector_;
    std::unordered_map<std::string, std::unique_ptr<IConnector>> connections_;

public:
    // Конструктор повторяет chc::ConnectorManager (3 параметра, 2 с defaults):
    ConnectorManager(actor_zeta::address_t catalog_manager,
                     connector_factory make_connector = make_file_connector,
                     size_t pool_size = std::thread::hardware_concurrency());

    void start();  // инициализирует thread pool
    void addConnection(connect_params params);
    void removeConnection(const std::string& uuid);

    // Асинхронно читает файл, возвращает future<T>
    // Сигнатура повторяет chc::ConnectorManager::executeQuery():
    //   принимает uuid, query (для файлов — пустая строка), handler
    template <typename Callable>
    requires std::invocable<Callable, const FileData&>
    auto executeQuery(const std::string& uuid, std::string_view query, Callable&& handler)
        -> std::future<std::invoke_result_t<Callable, const FileData&>>;
};

} // namespace filec
```

### 3.4 `db_integration/file/connection_manager.hpp`

```cpp
namespace db_conn {

class FileConnectionManager
    : public actor_zeta::cooperative_supervisor<FileConnectionManager> {

    std::shared_ptr<filec::ConnectorManager> connector_manager_;
    behavior_t execute_;    // route handler

    auto execute(session_hash_t                     id,
                 ParsedQueryDataPtr&&               data,
                 actor_zeta::address_t              scheduler) -> void;

    // Отправляет результат в scheduler через packaged_task + worker queue
    // (паттерн ChConnectionManager::send_result)
    void send_result(session_hash_t                     id,
                     ParsedQueryDataPtr&&               data,
                     actor_zeta::address_t              scheduler);
public:
    // actor_zeta handlers wired in constructor
};

} // namespace db_conn
```

**Логика `execute()`:**

```
1. Получить external_nodes из ParsedQueryData
2. Для каждого узла:
   a. uuid = node→collection_full_name.database  (uuid файлового соединения)
   b. connector_manager_->executeQuery(uuid, "", [&](const FileData& fd) {
          return tsl::parquet_to_chunk(resource(), fd.bytes.data(), fd.bytes.size());
          // или csv_to_chunk / json_to_chunk в зависимости от fd.format
      })
   c. Подождать future
3. Объединить все data_chunk_t в один (если несколько файлов — JOIN/UNION через otterbrix)
4. send_result(id, data, scheduler) — оборачивает отправку в packaged_task + worker queue,
   вызывает actor_zeta::send(scheduler, execute_remote_file_finish, id, data)
   (паттерн аналогичен ChConnectionManager::send_result())
```

---

### 3.5 S3 коннектор

S3-коннектор — файловый коннектор с дополнительным слоем скачивания через AWS SDK.
Структура полностью аналогична `filec`: свой `types.hpp` (Status enum, connect_params), свой `IConnector`, свой `ConnectorManager` (конструктор, members, `executeQuery` — как у `filec::ConnectorManager`).

```cpp
namespace s3c {

// Status enum — идентичен filec::Status и chc::Status:
// enum class Status : uint8_t { Created, Connected, Disconnected, Working, Closed };

struct connect_params {
    std::string bucket;
    std::string region;
    std::string access_key;
    std::string secret_key;
    std::string session_token;   // optional (для IAM-ролей)
    std::string prefix;          // путь внутри bucket (может содержать wildcard *.parquet)
    filec::FileFormat format;
    std::string alias;
};

// Промежуточный тип — данные, скачанные из S3 (аналог filec::FileData)
struct S3Data {
    std::vector<uint8_t>  bytes;
    filec::FileFormat     format;
    std::string           s3_key;     // для диагностики
};

// S3 определяет свой IConnector (каждый backend — свой namespace и свой IConnector)
class IConnector {
public:
    virtual ~IConnector() = default;
    virtual Status status() const noexcept = 0;
    virtual const connect_params& params() const noexcept = 0;
    virtual void connect() = 0;
    virtual bool isConnected() noexcept = 0;
    virtual void close() noexcept = 0;
    virtual bool isClosed() const noexcept = 0;
    virtual std::string alias() const noexcept = 0;
    virtual void tryReconnect() = 0;

    // Три перегрузки runQuery — паттерн аналогичен filec::IConnector и chc::IConnector
    virtual asio::awaitable<std::unique_ptr<components::vector::data_chunk_t>>
    runQuery(std::string_view query,
             std::function<std::unique_ptr<components::vector::data_chunk_t>(const S3Data&)> handler) = 0;

    virtual asio::awaitable<int64_t>
    runQuery(std::string_view query,
             std::function<int64_t(const S3Data&)> handler) = 0;

    virtual asio::awaitable<components::catalog::catalog_error>
    runQuery(std::string_view query,
             std::function<components::catalog::catalog_error(const S3Data&)> handler) = 0;
};

class S3Connector : public IConnector {
    connect_params params_;
    Status         status_;
    std::unique_ptr<Aws::S3::S3Client> s3_client_;

    // 1. ListObjects(bucket, prefix) → список ключей
    // 2. GetObject для каждого ключа → bytes
    // 3. Заполняет S3Data, передаёт в handler
    S3Data fetch_object(const std::string& key);
};

} // namespace s3c
```

---

## 4. Трансляторы (Backend-specific Result → data_chunk_t)

### 4.1 `otterbrix/translators/input/parquet_to_chunk.hpp`

Используем **Apache Arrow C++ library** (уже является зависимостью через otterbrix/Arrow Flight SQL).

```cpp
namespace tsl {

// Из файла на диске
components::vector::data_chunk_t
parquet_to_chunk(std::pmr::memory_resource* res,
                 const std::string& file_path);

// Из in-memory буфера (для S3)
components::vector::data_chunk_t
parquet_to_chunk(std::pmr::memory_resource* res,
                 const uint8_t* data, size_t size);

// Преобразовать схему Parquet → complex_logical_type (STRUCT)
components::types::complex_logical_type
parquet_to_struct(const std::string& file_path);

} // namespace tsl
```

**Реализация** использует `arrow::io::ReadableFile` + `parquet::arrow::FileReader` для чтения, получает `arrow::Table` / `arrow::RecordBatch`. Затем нужен новый транслятор `arrow_to_chunk()` (Arrow → data_chunk_t).

> **Важно:** существующий `chunk_to_arrow.hpp` работает только в одну сторону (chunk → Arrow schema). Обратного преобразования (`arrow::RecordBatch` → `data_chunk_t`) в кодовой базе нет. Его нужно реализовать:
> ```cpp
> // otterbrix/translators/input/arrow_to_chunk.hpp
> namespace tsl {
> components::vector::data_chunk_t
> arrow_to_chunk(std::pmr::memory_resource* res,
>                const std::shared_ptr<arrow::RecordBatch>& batch);
>
> components::types::complex_logical_type
> arrow_schema_to_struct(const std::shared_ptr<arrow::Schema>& schema);
> } // namespace tsl
> ```
> Этот модуль станет общей базой для всех трёх форматов (Parquet, CSV, JSON), так как все они читаются через Arrow readers и дают `arrow::RecordBatch`.

### 4.2 `otterbrix/translators/input/csv_to_chunk.hpp`

Используем **Arrow CSV reader** (`arrow/csv/reader.h`):

```cpp
namespace tsl {

components::vector::data_chunk_t
csv_to_chunk(std::pmr::memory_resource* res,
             const std::string& file_path,
             char delimiter = ',',
             bool has_header = true);

components::vector::data_chunk_t
csv_to_chunk(std::pmr::memory_resource* res,
             const uint8_t* data, size_t size,
             char delimiter = ',',
             bool has_header = true);

} // namespace tsl
```

### 4.3 `otterbrix/translators/input/json_to_chunk.hpp`

Используем **Arrow JSON reader** (`arrow/json/reader.h`):

```cpp
namespace tsl {

// Поддерживается NDJSON (newline-delimited JSON, один объект на строку)
components::vector::data_chunk_t
json_to_chunk(std::pmr::memory_resource* res,
              const std::string& file_path);

components::vector::data_chunk_t
json_to_chunk(std::pmr::memory_resource* res,
              const uint8_t* data, size_t size);

} // namespace tsl
```

**Важно:** Arrow JSON reader ожидает NDJSON (каждая строка — JSON объект). Для массива `[{...}, {...}]` нужна предобработка (читаем весь файл, разворачиваем массив).

---

## 5. Изменения в Scheduler

### `otterbrix/parser/parser.hpp`

`backend_type_t` определён в `otterbrix/parser/parser.hpp` (не в `scheduler/scheduler.hpp`):

```cpp
enum class backend_type_t : uint8_t {
    Unknown      = 0,
    MySQL        = 1,
    PostgreSQL   = 2,
    Mixed        = 3,
    Otterbrix    = 4,
    ClickHouse   = 5,
    File         = 6,  // NEW
    S3           = 7,  // NEW
};
```

### `routes/scheduler.hpp`

```cpp
enum class route {
    // ... существующие значения ...
    execute_remote_file_finish,   // NEW
    execute_remote_s3_finish,     // NEW
};
```

### Логика маршрутизации в `scheduler.cpp`

```cpp
// В execute_statement():
case backend_type_t::File:
    actor_zeta::send(file_connection_manager_,
                     address(),
                     file_conn::handler_id(file_conn::route::execute),
                     id, std::move(data), address());
    break;

case backend_type_t::S3:
    actor_zeta::send(s3_connection_manager_,
                     address(),
                     s3_conn::handler_id(s3_conn::route::execute),
                     id, std::move(data), address());
    break;
```

---

## 6. Изменения в CatalogManager

### `catalog/catalog_manager.hpp`

```cpp
enum class ConnectionType : uint8_t {
    MySQL      = 0,
    PostgreSQL = 1,
    ClickHouse = 2,
    File       = 3,  // NEW
    S3         = 4,  // NEW
};
```

Добавить методы регистрации:
```cpp
void set_file_connector_manager(std::shared_ptr<filec::ConnectorManager>);
void set_s3_connector_manager(std::shared_ptr<s3c::ConnectorManager>);
```

Логика определения `backend_type_t` в `update_backend_type()` должна учитывать новые типы при обходе `connection_registry_`.

---

## 7. Изменения в ComponentManager

### `component_manager/component_manager.cpp`

```cpp
// После существующих managers (паттерн: передаём address CatalogManager, как у chc/pgc/mysqlc):
file_connector_manager_ = std::make_shared<filec::ConnectorManager>(catalog_manager_->address());
s3_connector_manager_   = std::make_shared<s3c::ConnectorManager>(catalog_manager_->address());

catalog_manager_->set_file_connector_manager(file_connector_manager_);
catalog_manager_->set_s3_connector_manager(s3_connector_manager_);

file_connection_manager_ = actor_zeta::spawn_supervisor<FileConnectionManager>(
    environment(), file_connector_manager_);
s3_connection_manager_   = actor_zeta::spawn_supervisor<S3ConnectionManager>(
    environment(), s3_connector_manager_);

// Передать адреса в Scheduler (существующая сигнатура: res, parser, sql_addr, pg_addr, ch_addr, otterbrix_addr, catalog_addr):
scheduler_ = actor_zeta::spawn_supervisor<Scheduler>(
    ..., file_connection_manager_->address(), s3_connection_manager_->address());

// Вызвать start() на новых connector managers (по аналогии с db/pg/ch):
file_connector_manager_->start();
s3_connector_manager_->start();
```

---

## 8. HTTP API для регистрации соединений

Аналогично существующим (определены в `connectors/http_server/connection_server.cpp`):
- `POST /add_connection` — MySQL
- `POST /add_pg_connection` — PostgreSQL
- `POST /add_ch_connection` — ClickHouse
- `GET /check_connection`, `/check_pg_connection`, `/check_ch_connection` — проверка соединений

### `POST /add_file_connection`
```json
{
  "path": "/data/users.parquet",
  "format": "parquet",
  "alias": "users"
}
```

### `POST /add_s3_connection`
```json
{
  "bucket": "my-data-lake",
  "region": "us-east-1",
  "access_key": "AKIAIOSFODNN7EXAMPLE",
  "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "prefix": "analytics/events/*.parquet",
  "format": "parquet",
  "alias": "events"
}
```

После регистрации SQL запросы идут как обычно:
```sql
SELECT user_id, count(*) FROM users GROUP BY user_id;
SELECT * FROM events WHERE date > '2025-01-01';
```

---

## 9. Определение формата файла

В `filec::FileConnector::connect()` — если `format == Auto`:

```cpp
FileFormat detect_format(const std::string& path) {
    auto ext = std::filesystem::path(path).extension().string();
    if (ext == ".parquet") return FileFormat::Parquet;
    if (ext == ".csv" || ext == ".tsv") return FileFormat::CSV;
    if (ext == ".json" || ext == ".ndjson" || ext == ".jsonl") return FileFormat::JSON;
    // Попытаться прочитать magic bytes
    std::ifstream f(path, std::ios::binary);
    char magic[4];
    f.read(magic, 4);
    if (magic[0]=='P' && magic[1]=='A' && magic[2]=='R' && magic[3]=='1')
        return FileFormat::Parquet;
    return FileFormat::JSON; // fallback
}
```

---

## 10. Зависимости (Conan)

Нужно добавить в `conanfile.py` (или `conandata.yml` для otterstax):

| Пакет | Версия | Назначение |
|-------|--------|-----------|
| `arrow` | `21.0.0` (уже подключён) | Parquet/CSV/JSON reader + Flight SQL — нужно включить доп. опции |
| `aws-sdk-cpp` | `>=1.11` | S3 client (только subcomponents: `s3`, `core`) |

Arrow 21.0.0 уже подключён в `conanfile.py`, но **только с опциями для Flight SQL** (`with_flight_sql`, `with_flight_rpc`, `with_protobuf`, `with_grpc`, и компрессия). Для работы с файлами нужно добавить в `conanfile.py`:

```python
# В configure() или package_info():
self.options["arrow"].with_parquet = True    # Parquet reader/writer
self.options["arrow"].with_csv = True        # CSV reader
self.options["arrow"].with_json = True       # JSON reader
```

А в `CMakeLists.txt` добавить линковку:
```cmake
find_package(Parquet REQUIRED)   # arrow::parquet
# arrow::csv и arrow::json входят в arrow::arrow при включённых опциях
```

---

## 11. Порядок реализации (этапы)

### Этап 1 — Базовый File коннектор (Parquet)
1. `otterbrix/translators/input/arrow_to_chunk.hpp/cpp` — общий конвертер Arrow → data_chunk_t
2. `otterbrix/translators/input/parquet_to_chunk.hpp/cpp` — использует arrow_to_chunk
3. `connectors/file/types.hpp` + `connector.hpp/cpp` (только Parquet)
4. `connectors/file/manager.hpp/cpp`
5. `db_integration/file/connection_manager.hpp/cpp`
6. Обновить `otterbrix/parser/parser.hpp` (backend_type_t), `routes/scheduler.hpp`, `catalog_manager`, `component_manager`
7. Тест: `tests/unit/file/test_arrow_to_chunk.cpp`, `tests/unit/file/test_parquet_connector.cpp`

### Этап 2 — CSV и JSON
1. `csv_to_chunk.hpp/cpp`
2. `json_to_chunk.hpp/cpp`
3. Добавить поддержку форматов в `FileConnector`
4. Тест: `tests/unit/file/test_csv_connector.cpp`, `test_json_connector.cpp`

### Этап 3 — S3 коннектор
1. `connectors/s3/types.hpp` + `connector.hpp/cpp`
2. `connectors/s3/manager.hpp/cpp`
3. `db_integration/s3/connection_manager.hpp/cpp`
4. Обновить `scheduler`, `catalog_manager`, `component_manager`
5. Тест: `tests/integration/s3/test_s3_connector.cpp` (с mocked S3 через LocalStack)

### Этап 4 — HTTP API + конфигурация
1. HTTP handlers для `/add_file_connection`, `/add_s3_connection`
2. Поддержка в `config.hpp` (список файлов/S3-bucket'ов при старте сервера)
3. E2E тест через MySQL-клиент

---

## 12. Основные паттерны — краткий чеклист для реализации

- [ ] `IConnector` с 3 перегрузками `runQuery()` (data_chunk_t, int64_t, catalog_error) + handler с backend-специфичным типом
- [ ] `ConnectorManager` с `executeQuery(uuid, query, handler)` template (thread pool + co_spawn + future)
- [ ] `ConnectionManager` actor (наследует `actor_zeta::cooperative_supervisor`)
- [ ] Translator function `xxx_to_chunk()` и `xxx_to_struct()` для схемы
- [ ] `enum route` в `routes/<name>_connection_manager.hpp`
- [ ] `backend_type_t::File/S3` + `execute_remote_file/s3_finish` в scheduler routes
- [ ] `ConnectionType::File/S3` в CatalogManager
- [ ] Регистрация в `ComponentManager` конструкторе
- [ ] Передача адреса нового actor в `Scheduler`
- [ ] Unit-тест для транслятора
- [ ] System-тест для полного SQL-запроса через новый backend

---

## 13. Схема потока данных (File backend)

```
SQL Client
  │ SELECT * FROM events WHERE date > '2025-01-01'
  ▼
Scheduler::execute()
  │ parse() → backend_type = File
  ▼
Scheduler::execute_statement()
  │ actor_zeta::send → FileConnectionManager::execute()
  ▼
FileConnectionManager::execute()
  │ connector_manager_->executeQuery(uuid, "", handler)
  ▼
FileConnector::runQuery(query, handler)
  │ read_file() → FileData (сырые байты + формат)
  │ handler(FileData) → tsl::parquet_to_chunk() / csv_to_chunk() / json_to_chunk()
  │ returns unique_ptr<data_chunk_t>
  ▼
FileConnectionManager (получает data_chunk_t через future)
  │ actor_zeta::send(scheduler, execute_remote_file_finish, id, chunk)
  ▼
Scheduler::execute_remote_file_finish()
  │ вставляет chunk в OtterbrixStatement как external node data
  │ actor_zeta::send → OtterbrixManager::execute()
  ▼
OtterbrixManager::execute()
  │ data_manager_->execute_plan(params) — фильтрация/агрегация
  │ returns cursor
  ▼
Scheduler::execute_otterbrix_finish()
  │ complete_session() → Response to client
  ▼
SQL Client ← Results
```

---

## 14. Тестирование S3

Тестирование S3 backend следует существующим паттернам проекта: Catch2 (C++), hand-rolled mocks с `mock_config`, Python integration tests. Для интеграционных тестов используется **LocalStack** (эмулятор AWS S3 в Docker).

### 14.1 Структура тестовых файлов

```
tests/
├── mock/
│   └── s3_connector.hpp             # MockS3Connector (s3c::IConnector)
├── unit/
│   └── s3/
│       ├── CMakeLists.txt
│       ├── main.cpp
│       └── test_s3_connector.cpp    # Unit-тесты S3Connector (fetch, parse, error handling)
├── system/
│   └── test_scheduler.cpp           # + test cases для S3 backend routing
├── scripts/
│   ├── connection_s3.json           # Конфиг S3 backend для integration
│   ├── localstack_init.sh           # Создание bucket + загрузка test data
│   └── test_data/
│       ├── sample.parquet
│       ├── sample.csv
│       └── sample.ndjson
├── test_s3_backend.py               # Python E2E тесты (LocalStack)
└── create_s3_test_data.py           # Генерация тестовых файлов + upload в S3
```

### 14.2 Mock S3 коннектора

Следует паттерну `mock_config` + factory functions (как `tests/mock/sql_db_connector.hpp`, `tests/mock/ch_db_connector.hpp`):

**`tests/mock/s3_connector.hpp`:**

```cpp
#pragma once
#include "connectors/s3/connector.hpp"
#include "mock_config.hpp"

namespace s3c {

class MockS3Connector : public s3c::IConnector {
public:
    explicit MockS3Connector(mock_config config = {}, std::string alias = "mock_s3")
        : config_(config), alias_(alias) {}

    Status status() const noexcept override { return Status::Connected; }
    bool isConnected() noexcept override { return true; }
    void connect() override {}
    void close() noexcept override {}
    bool isClosed() const noexcept override { return false; }
    std::string alias() const noexcept override { return alias_; }
    const connect_params& params() const noexcept override { return params_; }
    void tryReconnect() override {}

    asio::awaitable<std::unique_ptr<components::vector::data_chunk_t>>
    runQuery(std::string_view query,
             std::function<std::unique_ptr<components::vector::data_chunk_t>(const S3Data&)> handler) override {
        if (config_.can_throw) {
            throw std::runtime_error(
                config_.error_message.empty() ? "MockS3Connector: exception in runQuery"
                                              : config_.error_message);
        }
        std::this_thread::sleep_for(config_.wait_time);

        S3Data sd;
        sd.format = filec::FileFormat::Parquet;
        sd.s3_key = "mock/test.parquet";

        if (config_.return_empty) {
            sd.bytes = {};
        } else {
            // Генерируем минимальный Parquet in-memory (2 строки: id + name)
            sd.bytes = generate_mock_parquet_bytes();
        }
        co_return handler(sd);
    }

    asio::awaitable<int64_t>
    runQuery(std::string_view query,
             std::function<int64_t(const S3Data&)> handler) override {
        S3Data sd;
        co_return handler(sd);
    }

    asio::awaitable<components::catalog::catalog_error>
    runQuery(std::string_view query,
             std::function<components::catalog::catalog_error(const S3Data&)> handler) override {
        S3Data sd;
        co_return handler(sd);
    }

private:
    mock_config config_;
    connect_params params_;
    std::string alias_;

    static std::vector<uint8_t> generate_mock_parquet_bytes() {
        // Создаём минимальный Parquet через Arrow in-memory
        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8())
        });
        arrow::Int64Builder id_b;
        arrow::StringBuilder name_b;
        (void)id_b.AppendValues({1, 2});
        (void)name_b.AppendValues({"alice", "bob"});
        auto table = arrow::Table::Make(schema, {*id_b.Finish(), *name_b.Finish()});

        auto sink = *arrow::io::BufferOutputStream::Create();
        (void)parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1024);
        auto buffer = *sink->Finish();
        return {buffer->data(), buffer->data() + buffer->size()};
    }
};

} // namespace s3c

// Factory functions (по паттерну make_mysql_mock_connector / make_ch_mock_connector):
inline std::unique_ptr<s3c::IConnector>
make_s3_mock_connector(s3c::connect_params params, std::string alias) {
    return std::make_unique<s3c::MockS3Connector>(mock_config{}, alias);
}

inline std::unique_ptr<s3c::IConnector>
make_s3_mock_connector_throw(s3c::connect_params params, std::string alias) {
    return std::make_unique<s3c::MockS3Connector>(mock_config{.can_throw = true}, alias);
}

inline std::unique_ptr<s3c::IConnector>
make_s3_mock_connector_empty(s3c::connect_params params, std::string alias) {
    return std::make_unique<s3c::MockS3Connector>(mock_config{.return_empty = true}, alias);
}
```

### 14.3 Unit-тесты S3 коннектора

**`tests/unit/s3/test_s3_connector.cpp`:**

```cpp
#include "connectors/s3/connector.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include <catch2/catch.hpp>

TEST_CASE("S3Data: parquet bytes → data_chunk_t via handler") {
    // Проверяем что S3Data с Parquet-байтами корректно парсится через handler
    auto* resource = std::pmr::get_default_resource();

    // Генерируем Parquet in-memory
    auto schema = arrow::schema({arrow::field("val", arrow::int64())});
    arrow::Int64Builder b;
    REQUIRE(b.Append(42).ok());
    auto table = arrow::Table::Make(schema, {*b.Finish()});
    auto sink = *arrow::io::BufferOutputStream::Create();
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1024).ok());
    auto buffer = *sink->Finish();

    s3c::S3Data sd;
    sd.bytes = {buffer->data(), buffer->data() + buffer->size()};
    sd.format = filec::FileFormat::Parquet;
    sd.s3_key = "test/data.parquet";

    // Handler конвертирует S3Data → data_chunk_t (как это делает S3ConnectionManager)
    auto chunk = tsl::parquet_to_chunk(resource, sd.bytes.data(), sd.bytes.size());
    REQUIRE(chunk.column_count() == 1);
    REQUIRE(chunk.size() == 1);
    REQUIRE(chunk.value(0, 0).value<int64_t>() == 42);
}

TEST_CASE("S3Data: csv bytes → data_chunk_t") {
    auto* resource = std::pmr::get_default_resource();
    std::string csv = "id,name\n1,alice\n2,bob\n";

    auto chunk = tsl::csv_to_chunk(resource,
                                    reinterpret_cast<const uint8_t*>(csv.data()), csv.size(),
                                    ',', true);
    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 2);
}

TEST_CASE("S3Data: json bytes → data_chunk_t") {
    auto* resource = std::pmr::get_default_resource();
    std::string ndjson = R"({"x":1})" "\n" R"({"x":2})" "\n";

    auto chunk = tsl::json_to_chunk(resource,
                                     reinterpret_cast<const uint8_t*>(ndjson.data()), ndjson.size());
    REQUIRE(chunk.column_count() == 1);
    REQUIRE(chunk.size() == 2);
}

TEST_CASE("S3Data: empty bytes") {
    auto* resource = std::pmr::get_default_resource();

    // Пустой буфер должен вернуть пустой chunk или ошибку
    REQUIRE_THROWS(tsl::parquet_to_chunk(resource, nullptr, 0));
}

TEST_CASE("S3Data: corrupted parquet") {
    auto* resource = std::pmr::get_default_resource();
    std::vector<uint8_t> garbage = {0x00, 0x01, 0x02, 0x03};

    REQUIRE_THROWS(tsl::parquet_to_chunk(resource, garbage.data(), garbage.size()));
}
```

**`tests/unit/s3/CMakeLists.txt`:**

```cmake
project(test_s3)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_s3_connector.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::otterbrix
    lib_otterstax
    Catch2::Catch2
    arrow::arrow
    Parquet::parquet_shared
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
```

Добавить в `tests/unit/CMakeLists.txt`:
```cmake
add_subdirectory(s3)
```

### 14.4 System-тесты (scheduler → S3 routing)

Добавить в `tests/system/test_scheduler.cpp` — по паттерну существующих test cases (`"base test case"`, `"Error in connector test case"`):

**Mock parser для S3:**

```cpp
// tests/mock/s3_parser.hpp
class S3MockParser : public IParser {
public:
    S3MockParser(mock_config config = {}) : config_(config) {}

    ParsedQueryDataPtr parse(const std::string& sql) override {
        if (config_.can_throw) {
            throw std::runtime_error(config_.error_message.empty()
                ? "S3MockParser: exception in parse" : config_.error_message);
        }
        std::this_thread::sleep_for(config_.wait_time);

        auto resource = std::pmr::get_default_resource();
        auto binder = sql::transform::transform_result(
            logical_plan::make_node_aggregate(resource, {"1", "s3bucket", "", "data"}),
            logical_plan::make_parameter_node(resource),
            {}, {}, data_chunk_t(resource, {}));

        auto parsed = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(
                std::vector<std::vector<logical_plan::node_ptr*>>{},
                binder.params_ptr(), binder.node_ptr(), 1),
            std::move(binder),
            NodeTag::T_SelectStmt);

        parsed->otterbrix_params->external_nodes.push_back({&parsed->otterbrix_params->node});
        parsed->backend_type = backend_type_t::S3;
        return parsed;
    }

private:
    mock_config config_;
};
```

**Test cases:**

```cpp
TEST_CASE("S3 backend: happy path") {
    using namespace std::chrono_literals;

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix();
    auto resource = std::pmr::get_default_resource();

    auto catalog_manager = actor_zeta::spawn_supervisor<mysqlc::CatalogManager>(resource);

    // S3 ConnectorManager с mock factory
    auto s3_conn_manager =
        std::make_shared<s3c::ConnectorManager>(catalog_manager->address(), make_s3_mock_connector);
    catalog_manager->set_s3_connector_manager(s3_conn_manager);
    s3_conn_manager->addConnection(s3c::connect_params{
        .bucket = "test-bucket", .region = "us-east-1",
        .prefix = "data/*.parquet", .format = filec::FileFormat::Parquet,
        .alias = "s3data"});

    // Остальные managers (mysql, pg, ch) с mock
    auto mysql_conn_manager = std::make_shared<mysqlc::ConnectorManager>(
        catalog_manager->address(), make_mysql_mock_connector);
    auto pg_conn_manager = std::make_shared<pgc::ConnectorManager>(
        catalog_manager->address(), make_pg_mock_connector);
    auto ch_conn_manager = std::make_shared<chc::ConnectorManager>(
        catalog_manager->address(), make_ch_mock_connector);
    catalog_manager->set_mysql_connector_manager(mysql_conn_manager);
    catalog_manager->set_pg_connector_manager(pg_conn_manager);
    catalog_manager->set_ch_connector_manager(ch_conn_manager);

    auto otterbrix_manager = actor_zeta::spawn_supervisor<db_conn::OtterbrixManager>(
        resource, std::make_unique<SimpleMockOtterbrixManager>());

    auto mysql_cm = actor_zeta::spawn_supervisor<db_conn::SqlConnectionManager>(resource, mysql_conn_manager);
    auto pg_cm = actor_zeta::spawn_supervisor<db_conn::PgConnectionManager>(resource, pg_conn_manager);
    auto ch_cm = actor_zeta::spawn_supervisor<db_conn::ChConnectionManager>(resource, ch_conn_manager);
    auto s3_cm = actor_zeta::spawn_supervisor<db_conn::S3ConnectionManager>(resource, s3_conn_manager);

    auto scheduler = actor_zeta::spawn_supervisor<Scheduler>(
        resource, std::make_unique<S3MockParser>(),
        mysql_cm->address(), pg_cm->address(), ch_cm->address(),
        s3_cm->address(),
        otterbrix_manager->address(), catalog_manager->address());

    session_id id;
    auto shared_data = create_cv_wrapper(session_payload(resource));

    actor_zeta::send(scheduler->address(), scheduler->address(),
                     scheduler::handler_id(scheduler::route::execute),
                     id.hash(), shared_data,
                     std::string("SELECT * FROM s3data"));

    shared_data->wait_for(5000ms);
    REQUIRE(shared_data->status() == cv_wrapper::Status::Ok);
}

TEST_CASE("S3 backend: connector error") {
    // Аналогичная setup, но s3_conn_manager с make_s3_mock_connector_throw
    // ...
    shared_data->wait_for(5000ms);
    REQUIRE(shared_data->status() == cv_wrapper::Status::Error);
    REQUIRE(shared_data->error_message() ==
        "Otterbrix execution failed: MockS3Connector: exception in runQuery");
}

TEST_CASE("S3 backend: empty result") {
    // s3_conn_manager с make_s3_mock_connector_empty
    // ...
    shared_data->wait_for(5000ms);
    REQUIRE(shared_data->status() == cv_wrapper::Status::Ok);
    // Пустой результат — не ошибка
}

TEST_CASE("S3 backend: cross-backend S3 + MySQL JOIN") {
    // S3MockParser возвращает backend_type_t::Mixed
    // node_backend_types: {"s3_uuid": S3, "mysql_uuid": MySQL}
    // Проверяем что Scheduler корректно маршрутизирует оба запроса
    // ...
}
```

### 14.5 Python integration-тесты (E2E с LocalStack)

LocalStack эмулирует AWS S3 локально в Docker. Тесты следуют паттерну `test_mysql_client_mysql_backend.py`.

**Docker Compose (добавить сервис):**

```yaml
# docker-compose.test.yml
services:
  localstack:
    image: localstack/localstack:3
    ports:
      - "4566:4566"
    environment:
      - SERVICES=s3
      - DEFAULT_REGION=us-east-1
```

**`tests/scripts/localstack_init.sh`** — загрузка тестовых данных в S3:

```bash
#!/bin/bash
AWS_ENDPOINT="http://localstack:4566"

# Создать bucket
aws --endpoint-url=$AWS_ENDPOINT s3 mb s3://test-bucket

# Загрузить тестовые файлы
aws --endpoint-url=$AWS_ENDPOINT s3 cp /data/test_data/sample.parquet s3://test-bucket/data/events.parquet
aws --endpoint-url=$AWS_ENDPOINT s3 cp /data/test_data/sample.csv s3://test-bucket/data/users.csv
aws --endpoint-url=$AWS_ENDPOINT s3 cp /data/test_data/sample.ndjson s3://test-bucket/data/logs.ndjson

echo "S3 test data uploaded"
```

**`tests/scripts/connection_s3.json`:**

```json
{
    "alias": "s3events",
    "bucket": "test-bucket",
    "region": "us-east-1",
    "access_key": "test",
    "secret_key": "test",
    "endpoint": "http://localstack:4566",
    "prefix": "data/events.parquet",
    "format": "parquet"
}
```

**`tests/test_s3_backend.py`:**

```python
"""
E2E тесты для S3 backend.
Требует: OtterStax + LocalStack (docker-compose.test.yml).
"""
from config import get_host, FLIGHT_PORT, HTTP_PORT
import pyarrow.flight as fl
import requests

def register_s3_connection(host, bucket, prefix, fmt, alias,
                           endpoint="http://localstack:4566"):
    url = f"http://{host}:{HTTP_PORT}/add_s3_connection"
    payload = {
        "bucket": bucket,
        "region": "us-east-1",
        "access_key": "test",
        "secret_key": "test",
        "endpoint": endpoint,
        "prefix": prefix,
        "format": fmt,
        "alias": alias
    }
    resp = requests.post(url, json=payload)
    assert resp.status_code in (200, 201), f"Register S3 failed: {resp.text}"

def query(host, sql):
    client = fl.FlightClient(f"grpc://{host}:{FLIGHT_PORT}")
    info = client.get_flight_info(fl.FlightDescriptor.for_command(sql.encode()))
    reader = client.do_get(info.endpoints[0].ticket)
    return reader.read_all()

# --- Тесты ---

def test_s3_parquet_select_all(host):
    """Чтение Parquet из S3 bucket."""
    register_s3_connection(host, "test-bucket", "data/events.parquet", "parquet", "s3events")
    table = query(host, "SELECT * FROM s3events")
    assert table.num_rows > 0, "S3 Parquet returned no rows"
    assert "id" in table.schema.names
    assert "event_name" in table.schema.names
    print(f"  s3_parquet_select_all: {table.num_rows} rows")

def test_s3_parquet_with_filter(host):
    """SELECT с WHERE (фильтрация через Otterbrix)."""
    table = query(host, "SELECT id, event_name FROM s3events WHERE id <= 10")
    assert table.num_rows == 10
    print(f"  s3_parquet_with_filter: {table.num_rows} rows")

def test_s3_parquet_aggregation(host):
    """GROUP BY через Otterbrix после загрузки из S3."""
    table = query(host, "SELECT campaign_id, count(*) as cnt FROM s3events GROUP BY campaign_id")
    assert table.num_rows > 0
    print(f"  s3_parquet_aggregation: {table.num_rows} groups")

def test_s3_csv(host):
    """Чтение CSV из S3."""
    register_s3_connection(host, "test-bucket", "data/users.csv", "csv", "s3users")
    table = query(host, "SELECT * FROM s3users")
    assert table.num_rows > 0
    assert "name" in table.schema.names
    print(f"  s3_csv: {table.num_rows} rows")

def test_s3_json(host):
    """Чтение NDJSON из S3."""
    register_s3_connection(host, "test-bucket", "data/logs.ndjson", "json", "s3logs")
    table = query(host, "SELECT level, count(*) FROM s3logs GROUP BY level")
    assert table.num_rows > 0
    print(f"  s3_json: {table.num_rows} groups")

def test_s3_cross_backend_join(host):
    """S3 JOIN MySQL — cross-backend запрос."""
    table = query(host,
        "SELECT e.event_name, c.campaign_name "
        "FROM s3events e "
        "JOIN campaigns.db1.schema.campaigns c "
        "ON e.campaign_id = c.campaign_id "
        "LIMIT 10")
    assert table.num_rows > 0
    assert "event_name" in table.schema.names
    assert "campaign_name" in table.schema.names
    print(f"  s3_cross_backend_join: {table.num_rows} rows")

def test_s3_nonexistent_key(host):
    """Запрос к несуществующему ключу — ожидаем ошибку."""
    register_s3_connection(host, "test-bucket", "does/not/exist.parquet", "parquet", "s3missing")
    try:
        query(host, "SELECT * FROM s3missing")
        assert False, "Expected error for missing S3 key"
    except Exception as e:
        assert "NoSuchKey" in str(e) or "not found" in str(e).lower()
        print(f"  s3_nonexistent_key: correctly raised error")

def test_s3_wildcard_prefix(host):
    """Чтение нескольких файлов по wildcard prefix."""
    register_s3_connection(host, "test-bucket", "data/*.parquet", "parquet", "s3wild")
    table = query(host, "SELECT * FROM s3wild")
    assert table.num_rows > 0
    print(f"  s3_wildcard_prefix: {table.num_rows} rows")

if __name__ == "__main__":
    import sys
    local = "--local" in sys.argv
    host = get_host(local)

    print("S3 Backend E2E Tests")
    print("=" * 50)
    test_s3_parquet_select_all(host)
    test_s3_parquet_with_filter(host)
    test_s3_parquet_aggregation(host)
    test_s3_csv(host)
    test_s3_json(host)
    test_s3_cross_backend_join(host)
    test_s3_nonexistent_key(host)
    test_s3_wildcard_prefix(host)
    print("=" * 50)
    print("All S3 backend tests passed")
```

Обновить `tests/scripts/add_connections.sh`:
```bash
S3_URL="http://${HOST}:8085/add_s3_connection"
send_connection_request "${S3_URL}" "connection_s3.json"
```

### 14.6 Тестовые данные для S3

Генерируются скриптом и загружаются в LocalStack:

**`tests/create_s3_test_data.py`:**

```python
import pyarrow as pa
import pyarrow.parquet as pq
import csv, json, os

out_dir = "tests/scripts/test_data"
os.makedirs(out_dir, exist_ok=True)

# Parquet: 100 строк events (campaign_id 1..50 для JOIN с MySQL campaigns)
table = pa.table({
    "id": range(1, 101),
    "event_name": [f"event_{i}" for i in range(100)],
    "campaign_id": [(i % 50) + 1 for i in range(100)],
    "timestamp": ["2025-01-01T00:00:00Z"] * 100,
})
pq.write_table(table, f"{out_dir}/sample.parquet")

# CSV: 50 строк users
with open(f"{out_dir}/sample.csv", "w") as f:
    w = csv.writer(f)
    w.writerow(["id", "name", "active"])
    for i in range(1, 51):
        w.writerow([i, f"user_{i}", i % 2 == 0])

# NDJSON: 200 строк logs
with open(f"{out_dir}/sample.ndjson", "w") as f:
    for i in range(200):
        level = ["INFO", "WARN", "ERROR"][i % 3]
        json.dump({"id": i, "level": level, "msg": f"log_{i}"}, f)
        f.write("\n")

print(f"Test data generated in {out_dir}/")
```

### 14.7 Матрица тестового покрытия S3

| Что тестируется | Unit (Catch2) | System (mocks) | E2E (LocalStack) |
|----------------|:---:|:---:|:---:|
| S3Data → parquet_to_chunk (from buffer) | x | | |
| S3Data → csv_to_chunk (from buffer) | x | | |
| S3Data → json_to_chunk (from buffer) | x | | |
| Пустой S3Data (empty bytes) | x | | |
| Битые данные (corrupted parquet) | x | | |
| Scheduler routing → S3ConnectionManager | | x | |
| S3 backend: happy path (mock) | | x | |
| S3 backend: ошибка коннектора | | x | |
| S3 backend: пустой результат | | x | |
| Cross-backend S3 + MySQL JOIN (mock) | | x | |
| SELECT * FROM S3 parquet | | | x |
| SELECT с WHERE фильтрацией | | | x |
| GROUP BY агрегация | | | x |
| S3 CSV чтение | | | x |
| S3 NDJSON чтение | | | x |
| Cross-backend S3 + MySQL JOIN (real) | | | x |
| Несуществующий S3 key → ошибка | | | x |
| Wildcard prefix (`data/*.parquet`) | | | x |
| HTTP API: `/add_s3_connection` | | | x |
| HTTP API: `/check_s3_connection` | | | x |
