package com.jrobertgardzinski.mail.entity;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

/**
 * Entity (BCE): the message to send — recipient, subject and plain-text body. Deliberately minimal;
 * templated/HTML mails and multiple recipients are future extensions. Validation runs at the
 * boundary ({@code @Valid} on the resource), so the control can assume a well-formed message.
 */
public record Mail(@NotBlank @Email String to, @NotBlank String subject, @NotBlank String text) {
}
