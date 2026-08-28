package com.jrobertgardzinski.mail.control;

import io.quarkus.runtime.LaunchMode;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import java.util.Set;

/**
 * Refuses a NORMAL start without an explicitly declared deployment profile. Quarkus defaults an
 * undeclared start to {@code prod} — the safest possible default, but still a decision nobody
 * made: the local stack runs this service under prod ON PURPOSE (real STARTTLS defaults, Mailpit
 * opted out explicitly), and that purpose belongs in the compose file as QUARKUS_PROFILE=prod,
 * not in a framework fallback. {@code quarkus:dev} and tests declare themselves through their
 * launch mode and pass untouched.
 */
@ApplicationScoped
public class ProfileGuard {

    private static final Set<String> DEPLOYMENT_PROFILES = Set.of("dev", "test", "prod");

    void refuseAnUndeclaredProfile(@Observes StartupEvent start) {
        if (LaunchMode.current() != LaunchMode.NORMAL)
            return;   // dev mode and tests carry their profile in the launch mode itself
        String declared = System.getenv().getOrDefault("QUARKUS_PROFILE",
                System.getProperty("quarkus.profile", ""));
        if (declared.isBlank())
            throw new IllegalStateException("no deployment profile declared - set"
                    + " QUARKUS_PROFILE=dev|prod explicitly; an undeclared start silently becomes"
                    + " prod, and a start nobody decided must not happen");
        if (!DEPLOYMENT_PROFILES.contains(declared))
            throw new IllegalStateException("unknown deployment profile '" + declared
                    + "' - declare one of dev|test|prod");
    }
}
