.. _bot:

Identification Bot
==================

File-reference
--------------
There are another endpoint `/details/x_edrInternalId` to process request to EDR to get detailed information using
parameter `x_edrInternalId`, received in first response. Only user from group `robot` have permissions to process
given request. Request looks like:

.. include:: tutorial/details.http
   :code:

File-reference name  is `edr_identification.yaml` and is uploaded to tenders with status ` active.pre-qualification` and
`procurementMethodType: aboveThresholdEU, competitiveDialogueUA, competitiveDialogueEU` and tender with status
`active.qualification` with `procurementMethodType: aboveThresholdUA, aboveThresholdUA.defense,
aboveThresholdEU, competitiveDialogueUA.stage2, competitiveDialogueEU.stage2`. Field `identifier.scheme` should have
value `UA-EDR`. File-reference is uploaded to `award/qualification` with `pending` status and contains field
`documentType: registerExtract`. If `identifier:id` is not found in EDR, then file will include next error
`{error: {code: notFound, errorDetails: Couldn't find this code in EDR.}}`.

File-reference also includes service information in `meta`.

* `author` - author of file-reference, have value `IdentificationBot`,
* `id` - unique identifier of file-reference,
* `sourceDate` - creation date,
* `sourceRequests` - list of request numbers to `API`, `EDR-proxy`,
* `version` - file-reference format version.

Value `meta.version` now have value 1.1.1. First number - major version (delete field, rename, remove), second number -
minor version (add field), third number - bugfix version (changes that do not change fields, but fix bugs).
For example, changing of file-reference name from `edr_request.yaml` to `edr_identification.yaml` increased
first number(major version), adding `meta.author` field increased second number (minor version).