'''
 extract_voters()---> function fetches voter data from a specific website.
 It takes five parameters representing location information, constructs a
 POST request to a predefined URL using provided form data, sends the request,
 and returns the HTML content of the response, likely containing the desired
'''

import requests
def extract_voters(state, district, vdc_mun, ward, reg_centre):
    data_endpoint = 'https://voterlist.election.gov.np/bbvrs1/view_ward_1.php'
    form_data = {
        'state': state,
        'district': district,
        'vdc_mun': vdc_mun,
        'ward': ward,
        'reg_centre': reg_centre
    }
    response = requests.post(data_endpoint, data=form_data)
    return response.content