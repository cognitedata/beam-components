#!/usr/bin/env bash

mvn clean package -Dimage=eu.gcr.io/cognitedata-development/replicate-assets \
                  -Dbase-container-image=gcr.io/dataflow-templates-base/java11-template-launcher-base \
                  -Dbase-container-image.version=latest \
                  -Dapp-root=/template/replicate-assets \
                  -Dcommand-spec=/template/replicate-assets/resources/replicate-assets-command-spec.json \
                  -am -pl templates/replicate-assets