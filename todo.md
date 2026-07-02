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
- **Realny SMTP** — konfiguracja hosta/portu/poświadczeń przez env na deployu (dziś dev=mock).
- **HTML** — dziś tylko plain text; te same szablony Qute mogą dostać wariant HTML.
- **Odporność** — kolejka + idempotencja ZROBIONE (Kafka at-least-once + dedup po id);
  otwarte: retry na błędy SMTP po stronie konsumenta, rate limiting.
