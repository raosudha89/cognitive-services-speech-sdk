#!/usr/bin/env python
# coding: utf-8

# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE.md file in the project root for full license information.

from typing import List

import argparse
import json
import os
import logging
import sys
import requests
import time
import swagger_client as cris_client

from azure.storage.blob import (
    BlockBlobService,
    BlobPermissions,
)

from datetime import datetime, timedelta

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(message)s")

# The subscription key must be for the region that you generated the Swagger
# client library for (see ../README.md for detailed instructions).
SUBSCRIPTION_KEY = ""

NAME = "Simple transcription"
DESCRIPTION = "Simple transcription description"

LOCALE = "en-US"
ACCOUNT_NAME = ""
ACCOUNT_KEY = ""
URI_PREFIX = "https://easip.blob.core.windows.net/commoncrawl-recipe-youtubevideos-audio"

# Set subscription information when doing transcription with custom models
ADAPTED_ACOUSTIC_ID = None  # guid of a custom acoustic model
ADAPTED_LANGUAGE_ID = None  # guid of a custom language model


def transcribe(outfile, dir_name, file_name):
    block_blob_service = BlockBlobService(account_name=ACCOUNT_NAME, account_key=ACCOUNT_KEY)
    container_name = "commoncrawl-recipe-youtubevideos-audio"
    blob_name = dir_name + '/' + file_name + '.wav' 
    sas_url = block_blob_service.generate_blob_shared_access_signature(
            container_name,
            blob_name,
            permission=BlobPermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
            start=datetime.utcnow()
        )
    print(sas_url)
    recordings_blob_uri = URI_PREFIX + '/' + blob_name + '?' + sas_url
    print(recordings_blob_uri)
    logging.info("Starting transcription client...")

    # configure API key authorization: subscription_key
    configuration = cris_client.Configuration()
    configuration.api_key['Ocp-Apim-Subscription-Key'] = SUBSCRIPTION_KEY

    # create the client object and authenticate
    client = cris_client.ApiClient(configuration)

    # create an instance of the transcription api class
    transcription_api = cris_client.CustomSpeechTranscriptionsApi(api_client=client)

    # get all transcriptions for the subscription
    transcriptions: List[cris_client.Transcription] = transcription_api.get_transcriptions()

    logging.info("Deleting all existing completed transcriptions.")

    # delete all pre-existing completed transcriptions
    # if transcriptions are still running or not started, they will not be deleted
    for transcription in transcriptions:
        try:
            transcription_api.delete_transcription(transcription.id)
        except ValueError:
            # ignore swagger error on empty response message body: https://github.com/swagger-api/swagger-core/issues/2446
            pass

    logging.info("Creating transcriptions.")

    # Use base models for transcription. Comment this block if you are using a custom model.
    # Note: you can specify additional transcription properties by passing a
    # dictionary in the properties parameter. See
    # https://docs.microsoft.com/azure/cognitive-services/speech-service/batch-transcription
    # for supported parameters.
    transcription_definition = cris_client.TranscriptionDefinition(
        name=NAME, description=DESCRIPTION, locale=LOCALE, recordings_url=recordings_blob_uri
    )

    # Uncomment this block to use custom models for transcription.
    # Model information (ADAPTED_ACOUSTIC_ID and ADAPTED_LANGUAGE_ID) must be set above.
    # if ADAPTED_ACOUSTIC_ID is None or ADAPTED_LANGUAGE_ID is None:
    #     logging.info("Custom model ids must be set to when using custom models")
    # transcription_definition = cris_client.TranscriptionDefinition(
    #     name=NAME, description=DESCRIPTION, locale=LOCALE, recordings_url=RECORDINGS_BLOB_URI,
    #     models=[cris_client.ModelIdentity(ADAPTED_ACOUSTIC_ID), cris_client.ModelIdentity(ADAPTED_LANGUAGE_ID)]
    # )

    data, status, headers = transcription_api.create_transcription_with_http_info(transcription_definition)

    # extract transcription location from the headers
    transcription_location: str = headers["location"]

    # get the transcription Id from the location URI
    created_transcription: str = transcription_location.split('/')[-1]

    logging.info("Checking status.")

    completed = False

    while not completed:
        running, not_started = 0, 0

        # get all transcriptions for the user
        transcriptions: List[cris_client.Transcription] = transcription_api.get_transcriptions()

        # for each transcription in the list we check the status
        for transcription in transcriptions:
            if transcription.status in ("Failed", "Succeeded"):
                # we check to see if it was one of the transcriptions we created from this client
                if created_transcription != transcription.id:
                    continue

                completed = True

                if transcription.status == "Succeeded":
                    results_uri = transcription.results_urls["channel_0"]
                    results = requests.get(results_uri)
                    logging.info("Transcription succeeded. Results: ")
                    with open(outfile, 'w', encoding='utf-8') as f:
                        json.dump(results.json(), f, ensure_ascii=False, indent=4)
                else:
                    logging.info("Transcription failed :{}.".format(transcription.status_message))
            elif transcription.status == "Running":
                running += 1
            elif transcription.status == "NotStarted":
                not_started += 1

        logging.info("Transcriptions status: "
                "completed (this transcription): {}, {} running, {} not started yet".format(
                    completed, running, not_started))

        # wait for 10 seconds
        time.sleep(10)

    #input("Press any key...")


def main(args):
    for dir_name in os.listdir(args.audio_files_dir):
        if not os.path.exists(os.path.join(args.output_files_dir, dir_name)):
            os.mkdir(os.path.join(args.output_files_dir, dir_name))
        for f_name in os.listdir(os.path.join(args.audio_files_dir, dir_name)):
            file_name = os.path.splitext(f_name)[0]
            outfile = os.path.join(args.output_files_dir, dir_name, file_name, ".json") 
            if os.path.exists(outfile):
                continue
            transcribe(outfile, dir_name, file_name)
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--audio_files_dir')
    parser.add_argument('--output_files_dir')
    args = parser.parse_args()
    main(args)

