# -*- coding: utf-8 -*-
from bottle import request, response
from simplejson import dumps


def setup_routing(app, func, path='/1.0/subjects', method='GET'):
    """ Setup routs """
    app.routes = []
    app.route(path, method, func)


def response_code():
    code = request.query.code
    if not code.isdigit() or not (len(code) == 10 or len(code) == 8):
        return dumps([])
    response.status = 200
    return dumps([{"code": code,
                   "name": "АКЦІОНЕРНЕ ТОВАРИСТВО КОМЕРЦІЙНИЙ БАНК \"ПРИВАТБАНК\"",
                   "url": "https://zqedr-api.nais.gov.ua/1.0/subjects/2842335",
                   "state": 1,
                   "state_text": "https://zqedr-api.nais.gov.ua/1.0/subjects/2842335",
                   "id": 2842335}])


def response_passport():
    passport = request.query.passport
    if passport.isdigit() and len(passport) == 11:
        response.status = 400
        return dumps({"errors": [{"code": 11, "message": "`passport` parameter has wrong value."}]})
    if passport.isalpha():
        response.status = 400
        return dumps({"errors": [{"code": 11, "message": "`passport` parameter has wrong value."}]})
    response.status = 200
    return dumps([{"code": passport,
                   "name": passport,
                   "url": "https://zqedr-api.nais.gov.ua/1.0/subjects/2842336",
                   "state": 1,
                   "state_text": "https://zqedr-api.nais.gov.ua/1.0/subjects/2842336",
                   "id": 2842336}])


def check_headers():
    if request.headers.get('Authorization') == 'Token':
        response.status = 401
        return dumps({"errors": [{"code": 1, "message": "Authentication credentials were not provided."}]})
    elif request.headers.get('Authorization') == 'Token 123':
        response.status = 401
        return dumps({"errors": [{"code": 2, "message": "Invalid or expired token."}]})


def payment_required():
    response.status = 402
    return dumps({"errors": [{"code": 5, "message": "Paiment required."}]})


def forbidden():
    response.status = 403
    return dumps({"errors": [{"code": 3, "message": "Your account is not permitted to access this resource."}]})


def not_found():
    response.status = 404
    return dumps({"errors": [{"code": 4, "message": "Sorry, that page does not exist."}]})


def not_acceptable():
    response.status = 406
    return dumps({"errors": [{"message": "Message."}]})


def too_many_requests():
    response.status = 429
    response.set_header('Retry-After', 26)
    return dumps({"errors": [{"code": 9, "message": "Request was throttled. Expected available in 26 seconds."}]})


def server_error():
    response.status = 500
    return dumps({"errors": [{"code": 20, "message": "Internal error."}]})


def bad_gateway():
    response.status = 502
    return dumps({"errors": [{"message": "Message."}]})


def two_error_messages():
    response.status = 404
    return dumps({"errors": [{"code": 0, "message": "Message1."}, {"code": 0, "message": "Message2."}]})


def response_details():
    response.status = 200
    return dumps({
      "id": 2842335,
      "state": 1,
      "state_text": "зареєстровано",
      "code": "14360570",
      "names": {
        "short": "ПАТ КБ \"ПРИВАТБАНК\"",
        "name": "КОМЕРЦІЙНИЙ БАНК \"ПРИВАТБАНК\"",
        "short_en": "PJSC CB \"PRIVATBANK\"",
        "include_olf": 1,
        "name_en": "PUBLIC JOINT-STOCK COMPANY COMMERCIAL BANK \"PRIVATBANK\"",
        "display": "АКЦІОНЕРНЕ ТОВАРИСТВО КОМЕРЦІЙНИЙ БАНК \"ПРИВАТБАНК\""
      },
      "olf_code": "230",
      "olf_name": "АКЦІОНЕРНЕ ТОВАРИСТВО",
      "founding_document": None,
      "executive_power": None,
      "object_name": "Реєстраційна служба",
      "founders": [
        {
          "capital": 18100740000,
          "role_text": "засновник",
          "name": "АКЦІОНЕРИ - ЮРИДИЧНІ ТА ФІЗИЧНІ ОСОБИ",
          "address": None,
          "role": 4
        }
      ],
      "management": "ЗАГАЛЬНІ ЗБОРИ",
      "heads": [
        {
          "appointment_date": None,
          "role_text": "підписант",
          "last_name": "Прізвище",
          "first_middle_name": "Імʼя По-батькові",
          "restriction": "Повноваження згідно положень статуту",
          "role": 2
        },
      ],
      "managing_paper": None,
      "is_modal_statute": False,
      "activity_kinds": [
        {
          "name": "Інші види грошового посередництва",
          "is_primary": True,
          "code": "64.19"
        },
        {
          "name": "Інші види кредитування",
          "is_primary": False,
          "code": "64.92"
        },
        {
          "name": "Надання інших фінансових послуг (крім страхування та пенсійного забезпечення), н. в. і. у.",
          "is_primary": False,
          "code": "64.99"
        },
        {
          "name": "Управління фінансовими ринками",
          "is_primary": False,
          "code": "66.11"
        },
        {
          "name": "Посередництво за договорами по цінних паперах або товарах",
          "is_primary": False,
          "code": "66.12"
        },
        {
          "name": "Інша допоміжна діяльність у сфері фінансових послуг, крім страхування та пенсійного забезпечення",
          "is_primary": False,
          "code": "66.19"
        }
      ],
      "branches": [
        {
          "type": 122,
          "name": "Ім'я",
          "address": {
            "country": "Країна",
            "address": "Адреса",
            "zip": ""
          },
          "code": "",
          "type_text": "Філія (інший відокремлений підрозділ)"
        }
      ],
      "address": {
        "country": "УКРАЇНА",
        "address": "Дніпропетровська обл., місто Дніпропетровськ, Жовтневий район ВУЛИЦЯ НАБЕРЕЖНА ПЕРЕМОГИ буд. 50",
        "zip": "49094"
      },
      "registraion": {
        "record_date": "1900-01-01",
        "is_transformation": False,
        "is_division": False,
        "date": "1900-01-01",
        "is_merge": False,
        "record_number": "",
        "is_separation": False
      },
      "bankruptcy": None,
      "termination": None,
      "termination_cancel": None,
      "assignees": [],
      "predecessors": [],
      "registrations": [
        {
          "start_date": "1900-01-01",
          "name": "ГОЛОВНЕ УПРАВЛІННЯ",
          "start_num": None,
          "end_num": None,
          "type": "",
          "code": "00000000",
          "end_date": None,
          "description": None
        },
      ],
      "primary_activity_kind": {
        "name": "Інші види грошового посередництва",
        "reg_number": "04030339603",
        "code": "64.19",
        "class": "5"
      },
      "prev_registration_end_term": None,
      "open_enforcements": [
        "1900-01-01",
        "1900-01-01"
      ],
      "contacts": {
        "tel": [
          "+38000000000",
          "+38000000000"
        ],
        "web_page": "www.privatbank.ua",
        "fax": "",
        "email": ""
      }
    })


def too_many_requests_details():
    response.status = 429
    response.set_header('Retry-After', 26)
    return dumps({"errors": [{"code": 9, "message": "Request was throttled. Expected available in 26 seconds."}]})


def bad_gateway_details():
    response.status = 502
    return dumps({"errors": [{"message": "Message."}]})
