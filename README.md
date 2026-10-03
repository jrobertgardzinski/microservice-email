# microservice-email

A small mail-sending microservice. Other services (e.g. `microservice-security`) POST a mail
command here and it delivers it. Deliberately a **different architecture** from the hexagonal
`microservice-security`, to show range:

## Architecture — Boundary / Control / Entity (Adam Bien style)

- **Boundary** (`boundary/MailResource`, `boundary/ApiKeyFilter`) — the REST entry point; thin,
  validates the command (Bean Validation) and guards access (shared API key).
- **Control** (`control/MailDispatcher`) — does the work: sends via Quarkus Mailer, rendering the
  templated mails (Qute); home for future logic (retries, rate limiting).
- **Entity** (`entity/Mail`, `entity/LinkMail`) — the messages (free-form, and single-action-link).

Infrastructure: **Quarkus** (`quarkus-rest-jackson` + `quarkus-mailer` + Qute templates). Tests use
Quarkus' `MockMailbox`, so no real SMTP is touched.

### What this service does NOT share with the email LIBRARIES (recorded 2026-10-03)

A layers review kept rediscovering this, so it is written down once. Two facts, both verified in
the code:

1. **The address invariants here are framework annotations, not domain types.**
   `entity/Mail` is `record Mail(@NotBlank @Email String to, …)` and `entity/LinkMail` is the
   same shape — `jakarta.validation.constraints`, checked by `@Valid` at the boundary, with the
   fields staying `String` inside.
2. **The `pom.xml` does not depend on `email-domain`** (nor on `constraint`). The mail-sending
   service therefore does not use the `Supplier<VO>` + `sealed Outcome` bridge that the
   `shared/email` and `shared/password` libraries exist to carry — no `Email` value object, no
   `InvalidEmailException` code, no `Outcome` variant.

**This is the BCE choice, not an oversight.** `shared/CLAUDE.md` names this service "the mail
service (`microservice-email`, BCE Quarkus)", and the point of BCE here is that the boundary is
the only validator: the control may assume a well-formed message, which is what the javadoc on
`Mail` says. A `to` field is an argument of an outbound command, not an aggregate this service
owns the lifecycle of — the account and its address live in `microservice-security`, which does
build `Email` through the library before the mail is ever requested.

So the shape is consistent with itself; the open question is only whether the owner wants the
kernel's bridge reused HERE for symmetry (gaining typed refusals and one vocabulary, paying with a
dependency from a BCE service onto a hexagon's library). **That is an architecture decision for
the owner — not something to refactor in passing.**

## Contract

Every request must present the shared secret in the `X-Api-Key` header (else `401` — the service is
internal, not an open relay). Malformed commands are refused with `400`.

```
POST /mails                                            # free-form plain-text mail
{ "to": "user@example.com", "subject": "Hi", "text": "Hello" }          -> 202 Accepted

POST /mails/verification                               # templated: e-mail verification link
{ "to": "user@example.com", "link": "https://app/verify?token=..." }    -> 202 Accepted

POST /mails/password-reset                             # templated: password-reset link
{ "to": "user@example.com", "link": "https://app/reset?token=..." }     -> 202 Accepted
```

`microservice-security` calls the two templated endpoints through its `EmailServiceClient`
(configured by `email-service.url` / `email-service.api-key` on the security side).

## Run & test

```bash
../mvnw -f pom.xml test          # unit/boundary tests (MockMailbox, JDK 25 + Quarkus 3.27)
../mvnw -f pom.xml quarkus:dev   # dev mode; mails are logged, not sent (quarkus.mailer.mock)
```

Production SMTP is configured at deploy via env vars, read by the `%prod` profile in
`application.properties`:

| env var | meaning | default |
|---|---|---|
| `MAIL_SMTP_HOST` | submission host | **required** (startup fails fast if unset) |
| `MAIL_SMTP_PORT` | submission port | `587` |
| `MAIL_SMTP_USERNAME` / `MAIL_SMTP_PASSWORD` | credentials | — |
| `MAIL_SMTP_START_TLS` | STARTTLS policy | `REQUIRED` |
| `MAIL_FROM` | envelope sender | `no-reply@jrobertgardzinski.com` |

Run with `-Dquarkus.profile=prod` (or `QUARKUS_PROFILE=prod`). Dev and test never touch a real
server — dev logs, tests use `MockMailbox`.
