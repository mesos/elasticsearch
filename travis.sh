#!/bin/bash
set -ev

./gradlew --info --stacktrace clean build system-test:systemTest
