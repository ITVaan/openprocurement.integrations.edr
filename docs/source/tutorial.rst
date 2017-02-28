.. _tutorial:

Tutorial
========

Basic request
---------------------

In order to get information from EDR by code EDRPOU you need to make a request to `/verify` endpoint, passing code as GET parameter:

.. include:: tutorial/basic_request.http
   :code:
Response consists of the following fields: `id`, `state`, `state_text`, `code`, `name`, `url`.

* `Id` - unique identification of the subject,
* `state` - state of the entity,
* `state_text` - text state of the entity,
* `code` - EDRPOU; if the subject - an individual entrepreneur - instead of IPN system returns ten zeros, because the data is confidential,
* `name` -  name of the entity,
* `url` - link to the entity with detailed information.

Request with Individual Tax Number
---------------------

If you need to obtain information about individual entrepreneurs then send a request with code IPN:

.. include:: tutorial/ipn.http
   :code:

Request with number of passport
---------------------

If due to religious beliefs a person refused to get IPN code, then you need to pass a series and number of passport, view like `АБ123456`:

.. include:: tutorial/passport.http
   :code:

If series and number of passport was passed in wrong format, then we get an error:

.. include:: tutorial/invalid_passport.http
   :code:

Errors
---------------------
Response to the unsuccessful request contains list of errors  with description and code in response body and status `403 Forbidden`.
If EDR service returns error, then code error presents in list of errors. In case list of errors contains only text message it means that an error occurred on our proxy server.

When given EDRPOU (IPN or series and number of passport) were not found in EDR response will contains message `EDRPOU not found`:

.. include:: tutorial/empty_response.http
   :code:
