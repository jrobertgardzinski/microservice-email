Feature: Sending mail on behalf of trusted services

  The mail service is internal: only callers presenting the shared API key may send a MAIL,
  and a malformed MAIL command is refused before anything is dispatched.

  Scenario: a trusted service sends a plain mail
    When a trusted service posts a mail to "user@example.com" with subject "Hi"
    Then the request is accepted
    And a mail with subject "Hi" is delivered to "user@example.com"

  Scenario: a caller without the API key is refused
    When an unknown caller posts a mail to "user@example.com" with subject "Hi"
    Then the request is refused as unauthorized
    And no mail is delivered

  Scenario: a malformed command is refused
    When a trusted service posts a mail with a blank recipient
    Then the request is refused as invalid
    And no mail is delivered

  Scenario: a verification link is rendered into the template
    When a trusted service requests a verification mail to "user@example.com" with link "https://app/verify?token=abc"
    Then the request is accepted
    And a mail with subject "Verify your email" is delivered to "user@example.com"
    And the delivered mail body contains "https://app/verify?token=abc"

  Scenario: a password-reset link is rendered into the template
    When a trusted service requests a password-reset mail to "user@example.com" with link "https://app/reset?token=xyz"
    Then the request is accepted
    And a mail with subject "Reset your password" is delivered to "user@example.com"
    And the delivered mail body contains "https://app/reset?token=xyz"
