# Подготовка отдельной shadow-cohort

Статус: **код подготовлен, cohort не создан и не запущен. Live BUY отключён.**

`shadow_cohort.py` отделён от `telemetry.py`. Действующие 14 дней telemetry
не меняются и не преобразуются в cohort задним числом: в них нет полного
статуса рынка на момент решения, исходов и зафиксированной 10-секундной опоры.

`shadow_runtime.py` — подготовленный сквозной адаптер для следующей,
отдельно утверждённой cohort. Он получает raw Gamma market object, две полные
CLOB-книги и синхронизированную Binance-котировку; затем строит
`ShadowSnapshot`, выбирает baseline и вызывает `record_shadow`. Ошибка любого
источника превращается в heartbeat с причиной, а не в сделку. В модуле нет CLI
и запуска по расписанию: cohort не может возникнуть из текущей telemetry
автоматически.

Перед регистрацией cohort нужен отдельный 30-минутный transport acceptance.
`FailoverBinanceDepthFeed` ведёт первичный Binance diff-depth endpoint и
явный fallback, наращивает backoff до заданного потолка и фиксирует каждый
disconnect и endpoint switch. Эти события сохраняются отдельно от торговых
наблюдений. Fallback допускается только если в acceptance он подтвердил тот же
контракт: millisecond event time `E`, непрерывная проверка update ID и правило
freshness 750 ms. Acceptance не создаёт cohort, не пишет PnL и не отправляет
ордера.

Воспроизводимая команда проверки источников:

```powershell
.\.venv\Scripts\python.exe preflight.py sources --duration-seconds 1800 `
  --endpoint-probe-seconds 15 --output reports\source_acceptance.json
```

После завершения действующей telemetry финальный quality gate проверяет
календарное покрытие, уникальность grain, максимальный разрыв, отрицательные
source lag, свежесть обоих источников и пропуски midpoint:

```powershell
.\.venv\Scripts\python.exe preflight.py final-quality `
  --db data\telemetry_calibration.sqlite3 `
  --run-id aee5c46a-b1a5-47ba-8eec-fef8ecc70e93 `
  --output reports\telemetry_final_quality.json
```

После завершения telemetry и отдельного утверждения параметров будущий сбор
создаёт полный `ShadowSnapshot` только после получения всех трёх источников.
В нём обязательны:

- raw `MarketState`: status, пара UP/DOWN, токены и текущий `feeSchedule`
  из актуального market/CLOB-ответа;
- обе полные, строго упорядоченные книги с payload source timestamps;
- Binance midpoint и event timestamp;
- measured local-minus-Binance clock offset and its uncertainty, used when
  evaluating the Binance event-time freshness gate;
- `FeatureBaseline`, который уже существовал к `t - 10 s`. Базовая точка не
  может быть будущей и не может отставать от `t - 10 s` больше чем на секунду.

Все константы, включая допуск опоры, объём виртуального входа в одну акцию и
максимальное удаление от best ask, входят в `LOCKED_PARAMETERS`. Создание
`CohortDefinition` требует фиксированное имя, точное совпадение lock,
письменную ссылку на approval и время начала. Иная гипотеза требует новой
версии кода, lock и cohort.

`record_shadow` сохраняет каждую направленную microshock-кандидатуру, включая
заблокированные. Все восемь verdicts и raw evidence остаются в отдельной
SQLite-базе. У accepted-кандидата сразу, до любого исхода, фиксируется один
представитель эпизода: первый по `(ts, snapshot_id)`. Поздно пришедший более
ранний accepted-снимок создаёт ошибку порядка, а не молча меняет выборку.

Отдельно каждый вызов будущего оценщика записывает `record_heartbeat`: и
полный снимок, и отсутствие кандидата, и невалидный/недоступный источник.
Кандидатуры сами по себе не доказывают непрерывность 30-дневного наблюдения.
`cohort_report` не считает cohort валидной, если от времени её старта до
`as_of_ts` есть промежуток без heartbeat длиннее 300 секунд. Доля полных
attempts и причины неполных остаются в отчёте как метрики качества данных;
неполный attempt не превращается в искусственно чистую строку.

`resolve_due_batches` запускается отдельно и обрабатывает ограниченное число
пакетов за один запуск. Исход рынка получает `make_polymarket_outcome_fetcher`
из публичного Gamma API. Закрытый рынок со спорным, неизвестным или неполным
статусом остаётся `PENDING`; void/refund фиксируется отдельно. Raw response
сохраняется. Сетевые ошибки получают backoff, после лимита переходят в
`RETRY_EXHAUSTED` и могут быть явно возвращены в очередь после восстановления
сервиса. Детерминированно испорченные цены quarantined по одной строке.

P&L — виртуальный результат одной акции: оценивается VWAP внутри `best ask +
1¢`, затем вычитается taker fee по опубликованной формуле. Это всё ещё не
исполнение: частичные fills, задержки, доступность лимитного ордера, проскальзывание
за видимой глубиной, выходы и инвентарный риск остаются темой отдельного
execution study.

`cohort_report(..., as_of_ts=...)` проверяет 30 календарных дней, heartbeat
coverage без разрыва более 300 секунд, минимум 100 resolved candidate rows и
30 resolved accepted episode representatives. Он
показывает net PnL по эпизодам, bootstrap-интервал, Wilson для win rate,
drop-best, leave-one-episode-out и leave-one-UTC-day-out. Это не позволяет
считать несколько связанных сигналов одного рыночного дня независимым
подтверждением. Диагностика удаления фильтров пересчитывает
cooldown в хронологическом порядке; microshock не атрибутируется, потому что
в cohort нет строк без microshock. Ни одно значение отчёта не включает live BUY.
