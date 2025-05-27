import io
import json
import logging
import oci
import re
import os

from fdk import response
from oci.signer import Signer

def compare_json(json1, json2, parent_key=""):
    added_values = ""
    changed_values = ""
    removed_values = ""
    
    # Treat as a list
    if isinstance(json1, list) and isinstance(json2, list):
        list_length = max(len(json1), len(json2))
        for index in range(list_length):
            key = str(index)
            # Access the elements on both sides to check for removed, changed, added values
            json1_value = json1[index] if index < len(json1) else None
            json2_value = json2[index] if index < len(json2) else None
            
            current_path = f"{parent_key}[{key}]" if parent_key else f"[{key}]"
            
            # Only process the removal of objects in list if that object exists
            if json1_value is not None and json2_value is None:
                removed_values = removed_values + f"Removed: {current_path} = {json1_value}" + "\n"
            
            # Both objects exists, compare them
            elif json1_value is not None and json2_value is not None:
                if isinstance(json1_value, dict) and isinstance(json2_value, dict):
                    added, changed, removed = compare_json(json1_value, json2_value, current_path)
                    added_values += added
                    changed_values += changed
                    removed_values += removed
                # Compare other values
                elif json1_value != json2_value:
                    changed_values = changed_values + f"Changed: {current_path} from {json1_value} to {json2_value}" + "\n"
            # Only process addition of object in list if the object doesn't exist in json1
            elif json1_value is None and json2_value is not None:
                added_values = added_values + f"Added: {current_path} = {json2_value}" + "\n"
    
    # Treat as a Dictionary if they are not lists
    elif isinstance(json1, dict) and isinstance(json2, dict):
        
        for key in json1:
            current_path = f"{parent_key}.{key}" if parent_key else key
            if key not in json2:
                removed_values = removed_values + f"Removed: {current_path} = {json1[key]}"
            
            elif isinstance(json1[key], dict) and isinstance(json2[key], dict):
                added, changed, removed = compare_json(json1[key], json2[key], current_path)
                added_values += added
                changed_values += changed
                removed_values += removed
            
            elif json1[key] != json2[key]:
                changed_values = changed_values + f"Changed: {current_path} from {json1[key]} to {json2[key]}"
        
        for key in json2:
            current_path = f"{parent_key}.{key}" if parent_key else key
            if key not in json1:
                added_values = added_values + f"Added: {current_path} = {json2[key]}"
    else:
        if json1 != json2:
            changed_values = changed_values + f"Changed: {parent_key} from {json1} to {json2}\n"
    
    return added_values, changed_values, removed_values


def pretty_print_result(result):
    formatted_result = []
    
    for entry in result:
        # Use regex to insert a newline before each "Added:", "Removed:", or "Changed:" if not at the start
        entry = re.sub(r'(Added:|Removed:|Changed:)', r'\n\1', entry)
        formatted_result.append(entry.strip())  # Strip to remove unnecessary leading/trailing spaces
    
    return  "\n".join(formatted_result)



def handler(ctx, data: io.BytesIO = None):

    try:
        
        signer = oci.auth.signers.get_resource_principals_signer()
        ons_client = oci.ons.NotificationDataPlaneClient(config={}, signer=signer)        
        
        topic_id =  os.getenv('topic_id')
        json_data = json.loads(data.getvalue())
        eventType = "\n Operation : " + json_data['eventType']
        eventTime = "\n Date & Time : " +  json_data['eventTime']


        userInfo = "\n \n User Information (Subject):\n"
        userInfo = userInfo + " " +"-" * 29
        userName= "\n User Name : " + json_data['data']['resourceName']
        userOCID = "\n User OCID : " + json_data['data']['resourceId']


        idDomainName = "\n Identity Domain Name: "+ json_data['data']['additionalDetails']['domainDisplayName']
        idDomain = "\n Identity Domain : " + json_data['data']['additionalDetails']['domainName']

        whoChanged = "\n \n Who changed it? (Actor):\n"
        whoChanged = whoChanged + " " + "-" * 29

        actor = "\n Actor Name : " +  json_data['data']['additionalDetails']['actorName']
        actor_displayName = "\n Actor Display Name : " + json_data['data']['additionalDetails']['actorDisplayName']
        

        changeInfo = "\n \n What's been changed?:\n"
        changeInfo = changeInfo + "-" * 29


        admin_values_added = json_data['data']['additionalDetails']['adminValuesAdded']
        admin_values_removed = json_data['data']['additionalDetails']['adminValuesRemoved']

        keys_to_remove = ['id', 'meta', 'ocid', 'userName','urn:ietf:params:scim:schemas:oracle:idcs:extension:user:User','idcsLastModifiedBy']

        for key in keys_to_remove:
                if key in admin_values_removed:
                   del admin_values_removed[key]
        for key in keys_to_remove:
                if key in admin_values_added:
                   del admin_values_added[key]
        
        
        change_details = compare_json(admin_values_removed, admin_values_added)

        change_details = pretty_print_result(change_details)

        result=  eventType + eventTime + userInfo + userName + userOCID + idDomainName + idDomain + whoChanged + actor + actor_displayName + changeInfo + "\n" + change_details + "\n"
        
        subject = userName + " : IAM User Update Event Details"
        message_details = oci.ons.models.MessageDetails(body=result,title=subject)
        ons_client.publish_message(topic_id,message_details)


    except (Exception) as ex:
        print('ERROR: Missing key in payload', ex, flush=True)
        raise

    return response.Response(
        ctx, response_data=result,
        headers={"Content-Type": "text/plain"}

     ) 
