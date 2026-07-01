# microservice-email

A small mail-sending microservice. Other services (e.g. `microservice-security`) POST a mail
command here and it delivers it. Deliberately a **different architecture** from the hexagonal
`microservice-security`, to show range:

## Architecture — Boundary / Control / Entity (Adam Bien style)

- **Boundary** (`boundary/MailResource`) — the REST entry point; thin, just accepts the command.
- **Control** (`control/MailDispatcher`) — does the work (send via Quarkus Mailer); home for future
  logic (templating, retries, rate limiting).
- **Entity** (`entity/Mail`) — the message (recipient, subject, text).

Infrastructure: **Quarkus** (`quarkus-rest-jackson` + `quarkus-mailer`). Tests use Quarkus'
`MockMailbox`, so no real SMTP is touched.

## Contract

```
POST /mails
Content-Type: application/json
{ "to": "user@example.com", "subject": "Verify your email", "text": "Click the link" }
-> 202 Accepted
```

## Run & test

```bash
../mvnw -f pom.xml test          # unit/boundary tests (MockMailbox, JDK 25 + Quarkus 3.20)
../mvnw -f pom.xml quarkus:dev   # dev mode; mails are logged, not sent (quarkus.mailer.mock)
```

Production SMTP is configured at deploy via env vars (`QUARKUS_MAILER_HOST`, `_PORT`,
`_USERNAME`, `_PASSWORD`); see `application.properties`.
