# -*- coding: utf-8 -*-
from bottle import request, response
from simplejson import dumps


SOURCEDATE = 'Tue, 25 Apr 2017 11:56:36 GMT'


def setup_routing(app, func, path='/1.0/subjects', method='GET'):
    """ Setup routs """
    app.routes = []
    app.route(path, method, func)


def response_code():
    code = request.query.code
    if not code.isdigit() or not (len(code) == 10 or len(code) == 8):
        response.content_type = 'application/json'
        response.headers['Date'] = SOURCEDATE
        return dumps([])
    response.status = 200
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps([{"code": code,
                   "name": "АКЦІОНЕРНЕ ТОВАРИСТВО КОМЕРЦІЙНИЙ БАНК \"ПРИВАТБАНК\"",
                   "url": "https://zqedr-api.nais.gov.ua/1.0/subjects/2842335",
                   "state": 1,
                   "state_text": "зареєстровано",
                   "id": 2842335}])


def response_passport():
    passport = request.query.passport
    if passport.isdigit() and len(passport) == 11:
        response.status = 400
        response.content_type = 'application/json'
        response.headers['Date'] = SOURCEDATE
        return dumps({"errors": [{"code": 11, "message": "`passport` parameter has wrong value."}]})
    if passport.isalpha():
        response.status = 400
        response.content_type = 'application/json'
        response.headers['Date'] = SOURCEDATE
        return dumps({"errors": [{"code": 11, "message": "`passport` parameter has wrong value."}]})
    response.status = 200
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps([{"code": passport,
                   "name": passport,
                   "url": "https://zqedr-api.nais.gov.ua/1.0/subjects/2842336",
                   "state": 1,
                   "state_text": "зареєстровано",
                   "id": 2842336}])


def check_headers():
    if request.headers.get('Authorization') == 'Token':
        response.status = 401
        response.content_type = 'application/json'
        response.headers['Date'] = SOURCEDATE
        return dumps({"errors": [{"code": 1, "message": "Authentication credentials were not provided."}]})
    elif request.headers.get('Authorization') == 'Token 123':
        response.status = 401
        response.content_type = 'application/json'
        response.headers['Date'] = SOURCEDATE
        return dumps({"errors": [{"code": 2, "message": "Invalid or expired token."}]})


def payment_required():
    response.status = 402
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({"errors": [{"code": 5, "message": "Payment required."}]})


def forbidden():
    response.status = 403
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({"errors": [{"code": 3, "message": "Your account is not permitted to access this resource."}]})


def not_found():
    response.status = 404
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({"errors": [{"code": 4, "message": "Sorry, that page does not exist."}]})


def not_acceptable():
    response.status = 406
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({"errors": [{"message": "Message."}]})


def too_many_requests():
    response.status = 429
    response.set_header('Retry-After', 26)
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({"errors": [{"code": 9, "message": "Request was throttled. Expected available in 26 seconds."}]})


def server_error():
    response.status = 500
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({"errors": [{"code": 20, "message": "Internal error."}]})


def bad_gateway():
    response.status = 502
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({"errors": [{"message": "Message."}]})


def two_error_messages():
    response.status = 404
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({"errors": [{"code": 0, "message": "Message1."}, {"code": 0, "message": "Message2."}]})


def response_details():
    response.status = 200
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
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
          "capital": None,
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
        "address": "Дніпропетровська обл., місто Дніпропетровськ, Жовтневий район",
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
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({"errors": [{"code": 9, "message": "Request was throttled. Expected available in 26 seconds."}]})


def bad_gateway_details():
    response.status = 502
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({"errors": [{"message": "Message."}]})


def wrong_ip_address():
    response.status = 403
    response.content_type = 'text/html'
    response.headers['Date'] = SOURCEDATE
    return '<html>\r\n<head><title>403 Forbidden</title></head>\r\n<body bgcolor="white">\r\n<center><h1>403 Forbidden</h1></center>\r\n<hr><center>nginx/1.10.1</center>\r\n</body>\r\n</html>\r\n'


def wrong_ip_address_detailed_request():
    response.status = 403
    response.content_type = 'text/html'
    response.headers['Date'] = SOURCEDATE
    return '<html>\r\n<head><title>403 Forbidden</title></head>\r\n<body bgcolor="white">\r\n<center><h1>403 Forbidden</h1></center>\r\n<hr><center>nginx/1.10.1</center>\r\n</body>\r\n</html>\r\n'


def null_fields():
    response.status = 200
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({
      "id": 2842335,
      "state": 1,
      "state_text": "зареєстровано",
      "code": "14360570",
      "olf_code": "230",
      "olf_name": "АКЦІОНЕРНЕ ТОВАРИСТВО",
      "founding_document": None,
      "executive_power": None,
      "object_name": "Реєстраційна служба",
      "founders": [
        {
          "capital": None,
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
        "address": "Дніпропетровська обл., місто Дніпропетровськ, Жовтневий район",
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


def sandbox_mode_data():
    response.status = 200
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps([{
        "url": "https://zqedr-api.nais.gov.ua/1.0/subjects/999186",
        "id": 999186,
        "state": 1,
        "state_text": "зареєстровано",
        "code": "00037256",
        "name": "ДЕРЖАВНЕ УПРАВЛІННЯ СПРАВАМИ"}])


def sandbox_mode_data_details():
    response.status = 200
    response.content_type = 'application/json'
    response.headers['Date'] = SOURCEDATE
    return dumps({
        "id": 999186,
        "state": 1,
        "state_text": "зареєстровано",
        "code": "00037256",
        "names": {
          "name": "ДЕРЖАВНЕ УПРАВЛІННЯ СПРАВАМИ",
          "short": "ДЕРЖАВНЕ УПРАВЛІННЯ СПРАВАМИ",
          "name_en": "",
          "include_olf": 0,
          "short_en": "",
          "display": "ДЕРЖАВНЕ УПРАВЛІННЯ СПРАВАМИ"
        },
        "olf_code": "425",
        "olf_name": "ДЕРЖАВНА ОРГАНІЗАЦІЯ (УСТАНОВА, ЗАКЛАД)",
        "founding_document": None,
        "executive_power": None,
        "object_name": "Відділ державної реєстрації юридичних осіб та фізичних осіб - підприємців Печерського району реєстраційної служби Головного управління юстиції у місті Києві",
        "founders": [
          {
            "address": None,
            "name": "УКАЗ ПРИЗИДЕНТА УКРАЇНИ №278/2000 ВІД 23 ЛЮТОГО 2000 РОКУ",
            "capital": 0,
            "role": 4,
            "role_text": "засновник"
          }
        ],
        "management": "КЕРІВНИК",
        "activity_kinds": [
          {
            "code": "84.11",
            "name": "Державне управління загального характеру",
            "is_primary": True
          }
        ],
        "address": {
          "address": "м.Київ, Печерський район ВУЛИЦЯ БАНКОВА буд. 11",
          "parts": {
            "num_type": None,
            "num": "",
            "house": "11",
            "street": "ВУЛИЦЯ БАНКОВА",
            "atu": "м.Київ, Печерський район",
            "building_type": None,
            "house_type": "буд.",
            "building": ""
          },
          "zip": "01220",
          "country": "УКРАЇНА"
        },
        "bankruptcy": None})

