package com.jrobertgardzinski.mail.entity;

/**
 * Entity (BCE): the message to send — recipient, subject and plain-text body. Deliberately minimal;
 * templated/HTML mails and multiple recipients are future extensions.
 */
public record Mail(String to, String subject, String text) {
}
