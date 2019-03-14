from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,Sequence
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from aiohttp import web
import aiohttp

CLEARBIT_KEY = ''
CLEARBIT_API = 'https://person.clearbit.com/v2/people/find'

# Full Contact constants
FULLCONTACT_KEY = ''
FULLCONTACT_API = 'https://api.fullcontact.com/v2/person.json'
FULLCONTACT_HEADERS = {'Authorization': "Bearer {}".format(FULLCONTACT_KEY)}

# DB Connection and session
engine = create_engine('sqlite:///:memory:', echo=True)
Session = sessionmaker(bind=engine)

Base = declarative_base()
class EmailInfo(Base):
    __tablename__ = 'emailinfo'
    id = Column(Integer, Sequence('emailinfo_id_seq'))  # Do we even need this field?
    email = Column(String(100), primary_key=True)
    name = Column(String(100))
    location = Column(String(100))
    angellist = Column(String(100))
    linkedin = Column(String(100))
    photo = Column(String(100))
    request_count = Column(Integer)

async def handle_email(request):
    email = request.rel_url.query['email']
    if not email:
        error_res = {"error": "no 'email' field in request"}
        return web.json_response(error_res, status=400)

    fullcontact_info = await fullcontact_lookup(email)
    clearbit_info = await clearbit_lookup(email)

    # Clearbit fields will overwrite Full Contact fields of same name.
    email_info = {'email': email, **fullcontact_info, **clearbit_info}
    stored_info = store_info(email_info)
    email_info['request_count'] = stored_info.request_count

    return web.json_response(email_info)

async def fullcontact_lookup(email):
    async with aiohttp.ClientSession() as client:
        # TODO(anaulin): Deal with HTTP errors gracefully.
        # and also deal with 404
        resp = await client.get("{}/?email={}".format(FULLCONTACT_API, email), headers=FULLCONTACT_HEADERS)
        resp_json = await resp.json()
        email_info = {
            # Assuming this field is always present. Not sure if that's warranted. TODO: verify.
            'name': resp_json['contactInfo']['fullName']
        }
        if 'photos' in resp_json and len(resp_json['photos']) > 0:
            email_info['photo'] = resp_json['photos'][0]['url']
        if 'demographics' in resp_json and 'locationGeneral' in resp_json['demographics']:
            email_info['location'] = resp_json['demographics']['locationGeneral']
        if 'socialProfiles' in resp_json:
            for profile in resp_json['socialProfiles']:
                if profile['type'] == 'angellist' and 'url' in profile:
                    email_info['angellist'] = profile['url']
                if profile['type'] == 'linkedin' and 'url' in profile:
                    email_info['linkedin'] = profile['url']
        return email_info

async def clearbit_lookup(email):
  #   async with aiohttp.ClientSession() as client:
  #       # TODO(anaulin): Deal with HTTP errors gracefully.
  #       resp = await client.get("{}/?email={}".format(FULLCONTACT_API, email), headers=FULLCONTACT_HEADERS)
  #       resp_json = await resp.json()
  #  # TODO: implement
  return {}

def store_info(info_dict):

  session = Session()
  try:
    info = session.query(EmailInfo).filter(EmailInfo.email==info_dict['email']).one()
  except:
    # Not found in DB. Start a new one.
    info = EmailInfo(email=info_dict['email'])

  if 'name' in info_dict:
    info.name = info_dict['name']
  if 'photo' in info_dict:
    info.photo = info_dict['photo']
  if 'linkedin' in info_dict:
    info.linkedin = info_dict['linkedin']
  if 'angellist' in info_dict:
    info.angellist = info_dict['angellist']
  if 'location' in info_dict:
    info.location = info_dict['location']

  if not info.request_count:
    info.request_count = 1
  else:
    info.request_count += 1

  session.add(info)
  session.commit()
  return info


if __name__ == "__main__":
    # Create DB tables.
    Base.metadata.create_all(engine)

    # Start web server.
    app = web.Application()
    app.add_routes([web.get('/', handle_email)])
    web.run_app(app)
