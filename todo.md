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
- **Spięcie z microservice-security** — security POST-uje tu przez `EmailServiceClient`
  (notifiery `Http*Notifier` zamiast placeholderów logujących).
- **Walidacja na boundary** — Bean Validation (`@NotBlank`, `@Email`); zniekształcona komenda → 400.
- **Dokumentacja jak w security** — kontrakt `/mails` w Gherkinie (`send-mail.feature`,
  quarkiverse quarkus-cucumber) + Allure (junit5 i cucumber7); glosariusz skanuje warstwy BCE.

## Otwarte
- **Realny SMTP** — konfiguracja hosta/portu/poświadczeń przez env na deployu (dziś dev=mock).
- **HTML** — dziś tylko plain text; te same szablony Qute mogą dostać wariant HTML.
- **Odporność** — retry/kolejka na błędy SMTP, idempotencja (id komendy), rate limiting.
