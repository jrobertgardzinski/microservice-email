package com.jrobertgardzinski.mail.entity;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

/**
 * Entity (BCE): a mail that carries a single action link to a recipient (used by the verification
 * and password-reset templated sends). Validation runs at the boundary ({@code @Valid} on the
 * resource), so the control can assume a well-formed message.
 */
public record LinkMail(@NotBlank @Email String to, @NotBlank String link) {
}
