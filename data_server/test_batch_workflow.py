import requests

# Configuration
api_base_url = "http://192.168.0.5:4280/apiv1/"  # Replace with your actual base url
api_access_key = "password4269"  # Replace with your actual API secret key

def get_work_batch(language, n):
    url = f"{api_base_url}/get_work_batch/{language}/{api_access_key}/{n}"
    response = requests.get(url)
    return response.json()

def register_wip_batch(wids):
    url = f"{api_base_url}/register_wip_batch/{api_access_key}"
    payload = {'wids': wids}
    response = requests.post(url, json=payload)
    return response.json()

def cancel_work_batch(wids):
    url = f"{api_base_url}/cancel_work_batch/{api_access_key}"
    payload = {'wids': wids}
    response = requests.post(url, json=payload)
    return response.json()

def test_workflow():
    # Step 1: Get a batch of work
    language = 'en'  # Example language
    batch_size = 4  # Example batch size
    work_batch = get_work_batch(language, batch_size)
    if work_batch['success']:
        print("Work batch fetched successfully:", work_batch)
        wids = [task['wid'] for task in work_batch['tasks']]

        # Step 2: Register work in progress for the fetched batch
        register_response = register_wip_batch(wids)
        if register_response['success']:
            print("Work registered successfully:", register_response)

            # Step 3: Cancel the work in progress
            cancel_response = cancel_work_batch(wids)
            if cancel_response['success']:
                print("Work cancelled successfully:", cancel_response)
            else:
                print("Failed to cancel work:", cancel_response)
        else:
            print("Failed to register work:", register_response)
    else:
        print("Failed to fetch work batch:", work_batch)

if __name__ == '__main__':
    test_workflow()

