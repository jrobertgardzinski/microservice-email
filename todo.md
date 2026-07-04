# TODO — microservice-email

Only open items. History = git log.

## Zrobione (walking skeleton)
- BCE (boundary/control/entity) + Quarkus. `POST /mails` → wysyłka przez `quarkus-mailer`.
- Test boundary z `MockMailbox` (bez realnego SMTP). Zielony na JDK 25 + Quarkus 3.27
  (release 25; 3.20 nie czytał bytecode'u Javy 25).
- **Autoryzacja endpointu** — wspólny sekret w nagłówku `X-Api-Key` (`ApiKeyFilter`,
  porównanie stałoczasowe); bez klucza 401.
- **Szablony** — Qute (`templates/verification.txt`, `templates/password-reset.txt`) +
  endpointy `POST /mails/verification` i `POST /mails/password-reset`.
- **Spięcie z microservice-security** — od 2026-07-02 asynchronicznie: security publikuje
  zdarzenia `mail-requests` przez Kafkę (transactional outbox), tu konsumuje je
  `MailRequestsConsumer` (reaktywnie — blokujący mailer w handlerze wiesza uporządkowaną
  kolejkę workerów Vert.x; dedup po id zdarzenia). REST `/mails*` zostaje dla klientów
  zewnętrznych.
- **Walidacja na boundary** — Bean Validation (`@NotBlank`, `@Email`); zniekształcona komenda → 400.
- **Dokumentacja jak w security** — kontrakt `/mails` w Gherkinie (`send-mail.feature`,
  quarkiverse quarkus-cucumber) + Allure (junit5 i cucumber7); glosariusz skanuje warstwy BCE.

## Otwarte
- **Realny SMTP** — kod gotowy (QUARKUS_MAILER_HOST/PORT/USERNAME/PASSWORD przez env,
  udokumentowane w application.properties); zostaje czynność wdrożeniowa: realne poświadczenia
  na produkcyjnym stacku.
- ~~HTML~~ — ZROBIONE (2026-07-04): każdy szablon ma wariant .html (multipart: plain text jako
  dostępny fallback + HTML z przyciskiem akcji); @Location jawnie nazywa sufiks.
- **Odporność** — kolejka + idempotencja + retry ZROBIONE: konsument ponawia błąd SMTP
  z backoffem (3 retry, 1s→8s), dedup zapamiętuje event dopiero PO wysłaniu (redelivery po
  crashu wciąż dostarcza), po wyczerpaniu prób loguje głośno i jedzie dalej (partycja się nie
  klinuje). RATE LIMITING — ZROBIONE (2026-07-04): stały limit/min na całym REST
  (`mail.rate-limit.per-minute`, env MAIL_RATE_LIMIT_PER_MINUTE, default 120; 0 wyłącza),
  429 + Retry-After; Kafka ma własne tempo (topik buforuje). DEAD-LETTER — ZROBIONE:
  po wyczerpaniu prób event PARKOWANY na `mail-requests-dlq` z awarią i liczbą prób
  (nic nie ginie po cichu; operator/re-drive znajdzie całość w jednym miejscu); test
  z zamockowanym dispatcherem i in-memory sinkiem. RE-DRIVE — ZROBIONY (2026-07-04): rejestr zaparkowanych czytany z topiku DLQ do
  ograniczonego ledgera (topik pozostaje trwałym zapisem), `GET /mails/dlq` listuje,
  `POST /mails/dlq/{id}/redrive` pcha event z powrotem NORMALNĄ ścieżką doręczenia (te same
  retry, to samo parkowanie gdy świat dalej leży); za kluczem API jak reszta.
