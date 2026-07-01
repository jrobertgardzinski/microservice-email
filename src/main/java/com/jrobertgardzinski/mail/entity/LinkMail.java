package com.jrobertgardzinski.mail.entity;

/**
 * Entity (BCE): a mail that carries a single action link to a recipient (used by the verification
 * and password-reset templated sends).
 */
public record LinkMail(String to, String link) {
}
