package com.jrobertgardzinski.mail.boundary.cucumber;

import io.quarkiverse.cucumber.CucumberOptions;
import io.quarkiverse.cucumber.CucumberQuarkusTest;

/**
 * Runs the mail contract feature through Cucumber inside a booted Quarkus test application,
 * reporting to Allure like the sibling services.
 */
@CucumberOptions(
        features = "src/test/resources/features",
        plugin = {"pretty", "io.qameta.allure.cucumber7jvm.AllureCucumber7Jvm"})
public class RunCucumberTest extends CucumberQuarkusTest {
}
