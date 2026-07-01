# TODO — microservice-email

Only open items. History = git log.

## Zrobione (walking skeleton)
- BCE (boundary/control/entity) + Quarkus. `POST /mails` → wysyłka przez `quarkus-mailer`.
- Test boundary z `MockMailbox` (bez realnego SMTP). Zielony na JDK 25 + Quarkus 3.20.

## Otwarte
- **Realny SMTP** — konfiguracja hosta/portu/poświadczeń przez env na deployu (dziś dev=mock).
- **Spięcie z microservice-security** — security ma dziś placeholder `Logging*Notifier`; podmienić na
  klienta HTTP POST-ującego `/mails` tutaj (albo kolejka zamiast HTTP — do decyzji).
- **Autoryzacja endpointu** — tylko zaufane serwisy mają móc wysyłać (token/mTLS/sieć wewnętrzna).
- **Szablony / HTML** — dziś tylko plain text; Quarkus ma Qute (już w zależnościach) do szablonów.
- **Odporność** — retry/kolejka na błędy SMTP, idempotencja (id komendy), rate limiting.
- **Dokumentacja jak w security** — cucumber (kontrakt `/mails`) + glosariusz. Uwaga: glosariusz z
  `build_glossary.py` skanuje `domain/config/*-system`; dla BCE trzeba dodać warstwy
  `boundary/control/entity` do skanera (drobna zmiana w generatorze).
