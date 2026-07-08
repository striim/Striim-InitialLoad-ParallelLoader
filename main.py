# This is a sample Python script.
import time
import requests
import datetime
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "loader"))

import config

import json
from data import (
    write_data,
    get_next_id,
    update_record,
    clear_runid,
    update_and_get_current_status,
    read_csv_to_query_results,
)


"""
This script contains a set of parameters listed below. Optionally, update this to use sys.argv[x] as indicated:
"""
prefixh = config.STRIIM_URL_PREFIX
node = config.STRIIM_NODE # Put your node IP Address or DNS name
username = config.STRIIM_ADMIN_USER # Use your ADMIN username here
password = config.STRIIM_ADMIN_PWD # User your ADMIN password here

polling_interval_seconds = config.APP_MONITOR_INTERVAL_SECONDS # This controls how often this will check for updates
log_output_path = config.LOG_OUTPUT_PATH # This indicates the path to store the output logs (persisted logging)

# Notes about the Code
# * This code is meant to be run as-is and be able to return valueable Initial Load or CDC Data.
# * This code is provided as a sample, in order to support being able to work with Striim's Rest API
# * This code is not officially supported as part of Striim

def authenticate():
    """Obtain a fresh Striim API token and return the authorization headers."""
    if config.STRIIM_API_TOKEN:
        return {'authorization': 'STRIIM-TOKEN ' + config.STRIIM_API_TOKEN, 'content-type': 'text/plain'}
    resp = None
    try:
        resp = requests.post(
            prefixh + node + '/security/authenticate',
            data={'username': username, 'password': password},
            timeout=30
        )
        jkvp = json.loads(resp.text)
        token = jkvp['token']
    except Exception as e:
        response_preview = resp.text[:200] if resp is not None else "(no response — connection failed)"
        raise SystemExit(
            f"Authentication failed. Check STRIIM_NODE and credentials. "
            f"Error: {e}\nResponse was: {response_preview}"
        )
    return {'authorization': 'STRIIM-TOKEN ' + token, 'content-type': 'text/plain'}

headers = {}  # Initialized in __main__ block via authenticate()

query_results = []

next_allowed_run = datetime.datetime.now()

class StriimApplication:
    def __init__(self, entity_type, full_name, status_change, rate, source_rate, cpu_rate, num_servers, latest_activity):
        self.entity_type = entity_type
        self.full_name = full_name
        self.status_change = status_change
        self.rate = rate
        self.source_rate = source_rate
        self.cpu_rate = cpu_rate
        self.num_servers = num_servers
        self.latest_activity = latest_activity
        self.components = []
        self.namespace = full_name.split('.')[0] if len(full_name.split('.')) > 1 else None

    def add_component(self, component):
        self.components.append(component)

class StriimClusterNode:
    def __init__(self, entity_type, name, version, free_memory, cpu_rate, uptime):
        self.entity_type = entity_type
        self.name = name
        self.version = version
        self.free_memory = free_memory
        self.cpu_rate = cpu_rate
        self.uptime = uptime

class Elasticsearch:
    def __init__(self, elasticsearchReceiveThroughput, elasticsearchTransmitThroughput, elasticsearchClusterStorageFree, elasticsearchClusterStorageTotal):
        self.elasticsearchReceiveThroughput = elasticsearchReceiveThroughput
        self.elasticsearchTransmitThroughput = elasticsearchTransmitThroughput
        self.elasticsearchClusterStorageFree = elasticsearchClusterStorageFree
        self.elasticsearchClusterStorageTotal = elasticsearchClusterStorageTotal

#
#  Usage: striim_apps, striim_nodes, es_nodes, response_valid = map_mon_json_response(json_response)
#

def map_mon_json_response(json_response):
    parsed_json = json_response
    striim_applications = []
    striim_cluster_nodes = []
    elasticsearch_nodes = []

    response_valid = True  # Assume valid initially

    # --- Section 1: Striim Applications (determines global response_valid) ---
    try:
        # Basic structural validation for striimApplications path
        if not (parsed_json and isinstance(parsed_json, list) and len(parsed_json) > 0 and
                isinstance(parsed_json[0], dict) and "output" in parsed_json[0] and
                isinstance(parsed_json[0]["output"], dict) and
                "striimApplications" in parsed_json[0]["output"] and
                isinstance(parsed_json[0]["output"]["striimApplications"], list)):
            response_valid = False
            applications_data = []
        else:
            applications_data = parsed_json[0]["output"]["striimApplications"]
            # If applications_data is an empty list, the loop below won't run,
            # and response_valid remains True, which is generally correct.
            # If an empty list of apps itself should be invalid, add:
            # if not applications_data: response_valid = False

        if response_valid:  # Only proceed if basic structure was okay
            for app_data in applications_data:
                if not isinstance(app_data, dict):
                    response_valid = False  # app_data item is not a dictionary
                    break

                # Critical check: "fullName" must exist and not be None
                if "fullName" not in app_data or app_data.get("fullName") is None:
                    response_valid = False
                    break  # One missing/None fullName invalidates the response

                # If fullName is present, proceed to create StriimApplication object.
                # The 'entityType' check is from your original code structure.
                if "entityType" in app_data:
                    striim_applications.append(
                        StriimApplication(
                            app_data["entityType"],
                            app_data["fullName"],  # Known to exist and be non-None
                            app_data.get("statusChange"),
                            app_data.get("rate"),
                            app_data.get("sourceRate"),
                            app_data.get("cpuRate"),
                            app_data.get("numServers"),
                            app_data.get("latestActivity")
                        )
                    )
                # If an app has fullName but no entityType, it's just not added to the list.
                # This does not make response_valid False by this simplified rule.

    except (TypeError, KeyError, IndexError) as e:
        # Error during parsing of striimApplications structure
        print(f"Error processing striimApplications: {e}")
        response_valid = False

    # If response is deemed invalid due to striimApplications, clear all lists
    if not response_valid:
        return [], [], [], False

    # --- Section 2: Striim Cluster Nodes (populated if response_valid is still True) ---
    try:
        if (parsed_json and isinstance(parsed_json, list) and len(parsed_json) > 0 and
                isinstance(parsed_json[0], dict) and "output" in parsed_json[0] and
                isinstance(parsed_json[0]["output"], dict) and
                "striimClusterNodes" in parsed_json[0]["output"] and
                isinstance(parsed_json[0]["output"]["striimClusterNodes"], list)):

            for node_data in parsed_json[0]["output"]["striimClusterNodes"]:
                if isinstance(node_data, dict):
                    # Using previous logic for adding nodes (e.g., entityType and version mandatory for addition)
                    if "entityType" in node_data and "version" in node_data:
                        striim_cluster_nodes.append(
                            StriimClusterNode(
                                node_data["entityType"],
                                node_data.get("name", node_data.get("fullName")),
                                node_data["version"],
                                node_data.get("freeMemory"),
                                node_data.get("cpuRate"),
                                node_data.get("uptime")
                            )
                        )
    except (TypeError, KeyError, IndexError) as e:
        print(f"Warning: Error processing striimClusterNodes: {e}")
        striim_cluster_nodes = []  # Clear if error, but doesn't make global response invalid here

    # --- Section 3: Elasticsearch Nodes (populated if response_valid is still True) ---
    try:
        if (parsed_json and isinstance(parsed_json, list) and len(parsed_json) > 0 and
                isinstance(parsed_json[0], dict) and "output" in parsed_json[0] and
                isinstance(parsed_json[0]["output"], dict) and
                "elasticsearch" in parsed_json[0]["output"] and
                isinstance(parsed_json[0]["output"]["elasticsearch"], dict)):
            es_data = parsed_json[0]["output"]["elasticsearch"]
            elasticsearch_nodes.append(
                Elasticsearch(
                    es_data.get("elasticsearchReceiveThroughput"),
                    es_data.get("elasticsearchTransmitThroughput"),
                    es_data.get("elasticsearchClusterStorageFree"),
                    es_data.get("elasticsearchClusterStorageTotal")
                )
            )
    except (TypeError, KeyError, IndexError) as e:
        print(f"Warning: Error processing elasticsearch: {e}")
        elasticsearch_nodes = []  # Clear if error

    return striim_applications, striim_cluster_nodes, elasticsearch_nodes, response_valid

# Example: update_application_components(applications[0], json_response)

def update_application_components(application, json_response):
    app_components = json.loads(json_response)[0]["output"]["striimApplications"][0]["applicationComponents"]
    for component in app_components:
        application.components.append(component)

def doDebugLog(text):
    logging.debug(text)

def runTQLFile(filePath, namespace, _depth=0):
    global headers

    fileContents = ""

    with open(filePath, 'r') as file:
        fileContents = file.read()

    print("Resetting namespace for use: " + namespace)
    isSuccessful, failuremessage = resetNamespace(namespace, True)

    data = 'USE ' + namespace + '; ' + fileContents

    print(data)

    try:
        resp = requests.post(prefixh + node + '/api/v2/tungsten', headers=headers, data=data)

        if 'reason' in resp.text and 'tkn' in resp.text:
            doDebugLog("got bad response in mon (tkn), trying again")
            if _depth >= 3:
                return False, 'Max retries exceeded'
            # If the response is bad, let's try again in 1 second
            time.sleep(1)
            headers = authenticate()
            return runTQLFile(filePath, namespace, _depth + 1)
        else:
            result = json.loads(resp.text)
            print(result)
            executionStatus = ""
            failureMessage = ""
            for row in result:
                if executionStatus != "Failure":
                    executionStatus = row.get('executionStatus')
                if executionStatus == "Failure":
                    failureMessage += row.get('failureMessage') + ";"
                    isSuccessful = False

            return isSuccessful, failureMessage
    except Exception as e:
        print('Error at runFilePath:', filePath, e)
        return False, str(e)

def resetNamespace(namespace, createNS=False):

    isSuccessful, failuremessage = runCommand('drop namespace ' + namespace + ' CASCADE;')

    isSuccessful, failuremessage = check_component_status(namespace, isSuccessful, failuremessage, "No objects", False)

    if createNS:
        ns_not_found = (failuremessage and
                        ("Cannot find" in failuremessage or "does not exist" in failuremessage))
        if isSuccessful or ns_not_found:
            isSuccessful, failuremessage = runCommand('create namespace ' + namespace + ';')
        # else: drop failed for a real reason; return that failure without trying to create

    return isSuccessful, failuremessage

def runCommand(strCmd, returnResultOnly=False, _depth=0):
    global headers

    if strCmd == '':
        return False, 'Empty command provided'

    stripped = strCmd.rstrip()
    data = stripped + ';' if not stripped.endswith(';') else stripped

    try:
        timeout_in_seconds = 180
        resp = requests.post(prefixh + node + '/api/v2/tungsten', headers=headers, data=data, timeout=timeout_in_seconds)
        # If passphrase is needed:
        # resp = requests.post(prefixh + node + '/api/v2/tungsten?passphrase=1234', headers=headers, data=data)

        if 'reason' in resp.text and 'tkn' in resp.text:
            doDebugLog("got bad response in mon (tkn), trying again")
            if _depth >= 3:
                return (False, 'Max retries exceeded') if not returnResultOnly else None
            # If the response is bad, let's try again in 1 second
            time.sleep(1)
            headers = authenticate()
            return runCommand(strCmd, returnResultOnly, _depth + 1)
        else:
            result = json.loads(resp.text)

            print(result)

            if returnResultOnly:
                return result

            failureMessage = ""
            executionStatus = "OK"

            for row in result:
                if executionStatus != "Failure":
                    executionStatus = row.get('executionStatus')
                if executionStatus == "Failure":
                    failureMessage += row.get('failureMessage') + ";"

            return (executionStatus != "Failure"), failureMessage
    except Exception as e:
        print('Error at runCommand:', strCmd, e)
        return False, 'Error occurred'

def runMon(component=''):
    data = 'mon;'
    if component != '':
        data = 'mon ' + component + ';'

    doDebugLog("Running mon for: " + data)

    return runCommand(data, True)

# This determines if a particular app is part of this Initial Load Automater Appset
def isILApp(full_name, ns_base=None):
    base = ns_base if ns_base is not None else config.ILA_NS_BASE
    segments = full_name.split('.')

    if len(segments) == 2:
        # Check if the first segment starts with the run's namespace base
        if segments[0].startswith(base):
            return True
    return False

def doGetMonOutputAndReview():
    start_time = time.time()
    response_valid = False
    MAX_DURATION_MINUTES = 15 # Run this again for up to 15 minutes before saying we failed.
    MAX_DURATION_SECONDS = MAX_DURATION_MINUTES * 60
    RETRY_DELAY_SECONDS = 30  # Time to wait before retrying if response is invalid

    print(f"Attempting to retrieve valid data for up to {MAX_DURATION_MINUTES} minutes...")

    striim_apps = []
    # Loop while the response is not valid AND time limit has not been reached
    while not response_valid and (time.time() - start_time < MAX_DURATION_SECONDS):
        elapsed_time_seconds = time.time() - start_time
        remaining_time_seconds = MAX_DURATION_SECONDS - elapsed_time_seconds

        print(f"Executing runMon(). Time remaining: {remaining_time_seconds:.0f}s")

        try:
            json_response = runMon()  # Your function to get data

            # Pass the response to the mapping function
            # It returns empty lists if response_valid_from_func is False
            s_apps_temp, s_nodes_temp, e_nodes_temp, response_valid_from_func = map_mon_json_response(json_response)

            if response_valid_from_func:
                print("Response from runMon() processed and is valid.")
                striim_apps = s_apps_temp
                response_valid = True  # Mark as valid to exit the loop
            else:
                print(f"Response from runMon() is invalid (e.g., missing 'fullName' in an application).")
                # Check if time permits another retry before sleeping
                if (time.time() - start_time + RETRY_DELAY_SECONDS < MAX_DURATION_SECONDS):
                    print(f"Waiting {RETRY_DELAY_SECONDS} seconds before retrying...")
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    print("Time limit approaching, will not attempt further retries after this.")
                    break  # Exit loop if next retry would exceed time limit

        except Exception as e:  # Catch potential errors from runMon() itself or unexpected issues
            print(f"An unexpected error occurred during runMon() or mapping: {e}")
            # Check if time permits another retry before sleeping
            if (time.time() - start_time + RETRY_DELAY_SECONDS < MAX_DURATION_SECONDS):
                print(f"Waiting {RETRY_DELAY_SECONDS} seconds before retrying after error...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                print("Time limit approaching, will not attempt further retries after this error.")
                break  # Exit loop

    # --- After the loop ---
    if response_valid:
        print(f"Successfully obtained valid data within {MAX_DURATION_MINUTES} minutes.")
        # Proceed to use striim_apps, striim_nodes, es_nodes
        # print(f"Striim Apps: {len(striim_apps)}, Nodes: {len(striim_nodes)}, ES: {len(es_nodes)}")
    else:
        elapsed_time_final = time.time() - start_time
        print(f"Failed to obtain valid data after {elapsed_time_final:.0f} seconds.")
        striim_apps = []  # Ensure empty list returned on failure
        # Handle this failure scenario as needed (e.g., log, raise exception, exit)

    return striim_apps

def runReview():
    # First, we need to check if there are any existing IL apps running.
    global query_results
    global next_allowed_run

    # Refresh state from DB to pick up any changes made since last iteration
    query_results = update_and_get_current_status()

    # Get node information: mon;

    striim_apps = doGetMonOutputAndReview()

    runningApps = 0

    # activeNamespace = config.ILA_NS_BASE + "1"

    # If this system is clean, it would fail if we don't confirm this has data
    if striim_apps:
        # Gather a count of running apps. We need this for two reasons:
        # -> To determine if we have reached max (based on config)
        # -> To determine the next namespace used (to prevent naming collisions / allow for easy cleanup)
        for app in [app for app in striim_apps if isILApp(app.full_name) and app.status_change in config.APP_RUNNING_STATUSES]:
            # Count apps running
            runningApps = runningApps + 1

        # print("Detected " + str(runningApps) + " apps running.")

        if runningApps == 0:
            next_allowed_run = datetime.datetime.now()

        # print("Namespace set to: " + activeNamespace)

        # Go through each app that fits our Initial Load app criteria (i.e. made by this program) in order to find completed
        for app in [app for app in striim_apps if isILApp(app.full_name)]:

            # Check if our log file indicates that there are any apps in Running
            # If so -> Check query_results for Running and NameSpace match
            for qry in [qry for qry in query_results if qry.status in config.RUNNING_STATUSES]:

                made_changes = False

                # Running Apps
                if app.namespace == qry.namespace:
                    # Detected that it is this row
                    if app.status_change == 'QUIESCED' or app.status_change == 'COMPLETED':
                        # Status change
                        qry.status = "COMPLETED"
                        qry.finished_datetime = datetime.datetime.now()
                        made_changes = True

                        encounteredFailure = False

                        # Undeploy and remove
                        isSuccessful, failuremessage = runCommand("UNDEPLOY APPLICATION " + qry.appname + ";")

                        isSuccessful, failuremessage = check_component_status(qry.appname, isSuccessful, failuremessage,
                                                                              "CREATED", False)

                        if not isSuccessful:
                            qry.status = "COMPLETED-FAILEDDROP"

                            # Ignore rest -----
                            qry.notes = (qry.notes or "") + ". FAILED UNDEPLOY; will try again."
                            isSuccessful, failuremessage = runCommand("UNDEPLOY APPLICATION " + qry.appname + ";")

                            isSuccessful, failuremessage = check_component_status(qry.appname, isSuccessful,
                                                                                  failuremessage,
                                                                                  "CREATED", False)

                            if not isSuccessful:
                                qry.notes = (qry.notes or "") + ". FAILED UNDEPLOY TWICE"
                                qry.status = "FAILED"
                                encounteredFailure = True
                        else:
                            isSuccessful, failuremessage = runCommand("DROP APPLICATION " + qry.appname + " CASCADE;")

                            isSuccessful, failuremessage = check_component_status(qry.appname, isSuccessful,
                                                                                  failuremessage,
                                                                                  "Cannot find", False)

                            if not isSuccessful:
                                qry.notes = (qry.notes or "") + ". FAILED DROP APPLICATION; will try again."
                                isSuccessful, failuremessage = runCommand("DROP APPLICATION " + qry.appname + " CASCADE;")

                                isSuccessful, failuremessage = check_component_status(qry.appname, isSuccessful,
                                                                                      failuremessage,
                                                                                      "Cannot find", False)

                                if not isSuccessful:
                                    qry.notes = (qry.notes or "") + ". FAILED DROP APPLICATION TWICE"
                                    qry.status = "FAILED"
                                    encounteredFailure = True

                        qry.notes = (qry.notes or "") + "; Total Execution time: " + pretty_time_difference(qry.started_datetime, qry.finished_datetime)

                        isSuccessful, failuremessage = resetNamespace(qry.namespace)
                        if not isSuccessful:
                            qry.notes = (qry.notes or "") + ". FAILED DROP NAMESPACE"
                            qry.status = "FAILED"

                if made_changes:
                    # If query changed, save this one
                    # Merge single row
                    new_result = update_record(qry, True)

                    # Update the record with the new information
                    for i in range(len(query_results)):
                        if query_results[i].id == qry.id:  # Assuming 'id' is unique
                            query_results[i] = new_result
                            break


    if datetime.datetime.now() < next_allowed_run:
        return

    # If so -> Check query_results for Running and NameSpace match
    for qry in sorted([qry for qry in query_results if qry.status not in config.NEW_EXCLUDES_STATUSES], key=lambda qry: qry.roworder):

        made_changes = False

        namespaceCount = runningApps + 1

        # Do stuff
        if runningApps < config.CONCURRENT_APPS_MAX:

            activeNamespace = config.ILA_NS_BASE + str(namespaceCount)

            namespaceCount = 1

            # Keep increasing namespace count until we get a unique one
            nsUsed = True
            while (nsUsed):
                nsUsed = False
                for app in striim_apps:
                    if app.namespace == activeNamespace:
                        nsUsed = True
                        namespaceCount = namespaceCount + 1
                        activeNamespace = (config.ILA_NS_BASE + str(namespaceCount))


            # Generate new TQL file from next entry in query_results
            newTQLFilePath = getNewFile(config.SOURCE_TQL_PATH, config.SOURCE_TQL_FILE, config.TARGET_TQL_PATH, qry.query, qry.targettbl, activeNamespace)

            # Should check here for success, or set up re-try
            isSuccessful, failuremessage = runTQLFile(newTQLFilePath, activeNamespace)

            isSuccessful, failuremessage = check_component_status(qry.appname, isSuccessful,
                                                                  failuremessage,
                                                                  "Cannot find", True)

            failPoint = ""

            fullAppName = activeNamespace + "." + config.ILA_APP_NAME_BASE

            qry.appname = fullAppName
            qry.namespace = activeNamespace

            made_changes = True

            if isSuccessful:

                # Deploy this new application
                # Should check here for success, or set up re-try
                isSuccessful, failuremessage = runCommand(f"DEPLOY APPLICATION {fullAppName} IN {config.DEPLOYMENT_GROUP_TARGET};")

                isSuccessful, failuremessage = check_component_status(fullAppName, isSuccessful,
                                                                      failuremessage,
                                                                      "DEPLOYED", False)

                if isSuccessful:
                    print("Deployment successful -> " + fullAppName)

                    try:
                        isSuccessful, failuremessage = runCommand("START APPLICATION " + fullAppName + ";")

                        isSuccessful, failuremessage = check_component_status(qry.appname, isSuccessful,
                                                                              failuremessage,
                                                                              "DEPLOYED", True)

                        if isSuccessful:
                            print("Start App successful -> " + fullAppName)
                            qry.status = 'RUNNING'
                            qry.started_datetime = datetime.datetime.now()
                        else:
                            failPoint = "START"
                            qry.status = "FAILED"
                            qry.notes = (qry.notes or "") + "Start App Failed: " + failuremessage
                    except Exception as e:
                        striim_apps2 = doGetMonOutputAndReview()

                        failPoint = "START"
                        qry.status = "FAILED"

                        for app in [app for app in striim_apps2 if
                                    app.full_name == fullAppName and app.status_change in config.APP_RUNNING_STATUSES]:
                            # Update query results with LOADED and NS
                            qry.status = 'RUNNING'
                            qry.started_datetime = datetime.datetime.now()
                else:
                    failPoint = "DEPLOY"
                    qry.notes = (qry.notes or "") + "Unable to deploy: " + failuremessage
                    qry.status = "FAILED"
                    qry.finished_datetime = datetime.datetime.now()
                    print(qry.notes)

                # Start this new application
                # Should check here for success, or set up re-try
            else:
                failPoint = "CREATE"
                qry.notes = (qry.notes or "") + "Unable to create: " + failuremessage
                qry.status = "FAILED"
                qry.finished_datetime = datetime.datetime.now()
                print(qry.notes)


            next_allowed_run = datetime.datetime.now() + datetime.timedelta(seconds=config.DEPLOY_WAIT_TIME_SECONDS)

            if qry.status == "FAILED":
                print("Attempting cleanup of apps:")
                if (failPoint == "START"):
                    isSuccessful, failuremessage = runCommand("UNDEPLOY APPLICATION " + fullAppName + ";")

                    isSuccessful, failuremessage = check_component_status(fullAppName, isSuccessful,
                                                                          failuremessage,
                                                                          "CREATED", False)

                    if isSuccessful:
                        qry.notes = (qry.notes or "") + " [Cleanup: Able to UNDEPLOY]"
                    else:
                        qry.notes = (qry.notes or "") + " [Cleanup Failure: Unable to UNDEPLOY: -> " + failuremessage + "]"
                #if (failPoint == "DEPLOY"):
                isSuccessful, failuremessage = resetNamespace(qry.namespace)

        if made_changes:
            # If query changed, save this one
            # Merge single row
            new_result = update_record(qry, True)

            for i in range(len(query_results)):
                if query_results[i].id == qry.id:
                    query_results[i] = new_result
                    break

        # Do only one change at a time
        break


def pretty_time_difference(date1, date2):
    """
    Calculates the time difference between two datetime objects and returns a formatted string.

    Args:
        date1 (datetime.datetime): The first datetime.
        date2 (datetime.datetime): The second datetime.

    Returns:
        str: A formatted string showing the time difference in hours, minutes, and seconds.
    """
    if date1.tzinfo is None:
        date1 = date1.replace(tzinfo=datetime.timezone.utc)
    if date2.tzinfo is None:
        date2 = date2.replace(tzinfo=datetime.timezone.utc)

    time_difference = date2 - date1
    total_seconds = abs(time_difference.total_seconds())

    hours, remainder = divmod(int(total_seconds), 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours} {'hour' if hours == 1 else 'hours'}")
    if minutes > 0:
        parts.append(f"{minutes} {'minute' if minutes == 1 else 'minutes'}")
    if seconds > 0 or not parts:
        parts.append(f"{seconds} {'second' if seconds == 1 else 'seconds'}")

    return ", ".join(parts)

def check_component_status(objectName, currentIsSuccessful, currentFailureMessage, expectedStatus, invertExpectation):
    """
    Checks the status of an application and updates the success status and failure message accordingly.

    Args:
      appName: The name of the application.
      currentFailureMessage: The current failure message.
      expectedStatus: The expected status of the application.
      invertExpectation: A boolean indicating whether to invert the expectation of the status.

    Returns:
      A tuple containing the updated success status (boolean) and failure message (string or None).
    """
    isSuccessful = currentIsSuccessful
    failuremessage = currentFailureMessage

    for _ in range(5):  # Loop 5 times to check for 503 errors
        if not isSuccessful and failuremessage and ("503" in failuremessage or "Connection aborted" in failuremessage):
            print("503 error, checking status for " + objectName)
            result = runCommand("STATUS " + objectName + ";", True)

            if result is None:
                break

            result_str = str(result)

            if invertExpectation:
                if expectedStatus not in result_str:
                    print("STATUS confirmed success for " + objectName)
                    isSuccessful = True
                    failuremessage = None
            else:
                if expectedStatus in result_str:
                    print("STATUS confirmed success for " + objectName)
                    isSuccessful = True
                    failuremessage = None

            if "503" in result_str or "Connection aborted" in result_str:
                print("503 error, retrying status check for " + objectName)
                time.sleep(5)
            else:
                break
        else:
            break  # Not a 503 error, no recovery needed

    return isSuccessful, failuremessage

def cleanNamespace(targetPath, namespace):
    for item in os.listdir(targetPath):
        path = os.path.join(targetPath, item)
        if item.startswith(namespace + '_') and os.path.isfile(path):
            os.remove(path)

# Map a configured DB engine (config.SOURCE_DB_TYPE / TARGET_DB_TYPE) to the exact
# string Striim's DatabaseReader/DatabaseWriter expect for DatabaseProviderType.
_STRIIM_PROVIDER = {"oracle": "Oracle", "postgres": "PostgreSQL", "postgresql": "PostgreSQL",
                    "pg": "PostgreSQL", "sqlserver": "SQLServer", "mssql": "SQLServer"}

def striim_provider_type(db_type, env_override=""):
    if env_override:
        return env_override
    key = (db_type or "oracle").lower()
    if key in _STRIIM_PROVIDER:
        return _STRIIM_PROVIDER[key]
    # jdbc / unknown: provider is source-specific — require an explicit override.
    raise ValueError(f"No Striim DatabaseProviderType mapping for '{db_type}'. "
                     f"Set STRIIM_SOURCE_PROVIDER_TYPE / STRIIM_TARGET_PROVIDER_TYPE.")

def getNewFile(sourcePath, sourceFileName, targetPath, queryText, targetTable, namespace):
    os.makedirs(targetPath, exist_ok=True)
    fullPath = os.path.join(sourcePath, sourceFileName)
    src_provider = striim_provider_type(config.SOURCE_DB_TYPE, os.environ.get("STRIIM_SOURCE_PROVIDER_TYPE", ""))
    tgt_provider = striim_provider_type(config.TARGET_DB_TYPE, os.environ.get("STRIIM_TARGET_PROVIDER_TYPE", ""))
    with open(fullPath, "rt") as fin:
        content = fin.read()
        modified_content = content.replace('~QUERYTEXT~', queryText).replace('~TARGETTABLE~', targetTable).replace('~PROVIDER_TYPE_SRC~', src_provider).replace('~PROVIDER_TYPE_TGT~', tgt_provider)

    cleanNamespace(targetPath, namespace)

    fullTargetPath = os.path.join(targetPath, namespace + '_' + sourceFileName)
    with open(fullTargetPath, "wt") as fout:
        fout.write(modified_content)

    return fullTargetPath

def doNSClean():

    striim_apps =  doGetMonOutputAndReview()

    # Go through each app that fits our Initial Load app criteria (i.e. made by this program) in order to find completed
    for app in [app for app in striim_apps if isILApp(app.full_name)]:
        print(runCommand("STOP APPLICATION " + app.full_name + ";"))
        print(runCommand("UNDEPLOY APPLICATION " + app.full_name + ";"))
        print(runCommand("DROP APPLICATION " + app.full_name + " CASCADE;"))
        try:
            resetNamespace(app.namespace)
        except Exception as e:
            print('Error at resetNamespace:', e)


if __name__ == '__main__':
    headers = authenticate()

    log_dir = os.path.dirname(config.LOG_OUTPUT_PATH)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    log_level = logging.DEBUG if os.environ.get('DEBUG') else logging.INFO
    logging.basicConfig(filename=log_output_path, level=log_level, format='%(asctime)s:%(levelname)s:%(message)s')

    # Easy way to disable running
    run = True

    if run:
        import sys
        import run_safety

        _force_fresh = ('--force-fresh' in sys.argv) or bool(os.environ.get('FORCE_FRESH'))

        # Singleton run-lock: refuse a second concurrent loader on this run id.
        _lock_ok, _lock_msg = run_safety.acquire_lock(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
        print(_lock_msg)
        if not _lock_ok:
            sys.exit(1)

        # Current rows for this run id from the state backend.
        query_results = update_and_get_current_status()

        # RESUME / FRESH / REFUSE from the row count + the run marker.
        _qf_hash = run_safety.queryfile_hash(config.QUERY_FILE_PATH) if os.path.exists(config.QUERY_FILE_PATH) else ''
        _marker = run_safety.read_marker(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
        _decision, _reason = run_safety.decide_startup(
            len(query_results), _marker, config.STAGE_DB_LOCATION, _qf_hash, _force_fresh
        )
        print('[startup] ' + _decision + ': ' + _reason)
        if _decision == 'REFUSE':
            run_safety.release_lock(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
            sys.exit(1)

        _lock_released = False

        # FRESH path: (re)read the queryfile and deploy. (RESUME skips this.)
        if _decision == 'FRESH':
            query_results = read_csv_to_query_results()

            # Get next ID available
            next_id = get_next_id()
            # Assign IDs to new rows without IDs
            for qr in query_results:
                qr.id = next_id
                qr.uniquerunid = config.UNIQUE_RUN_ID
                qr.notes = ""
                qr.status = "NEW"
                next_id = next_id + 1

            # Persist these rows
            write_data(query_results)

            # Marker written AFTER write_data succeeds (a crash mid-fresh-start must not
            # wedge the retry). Deleted on ALL_COMPLETE completion + in `manage.py clear`.
            run_safety.write_marker(
                config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID,
                config.STAGE_DB_LOCATION, _qf_hash, len(query_results),
            )

            # Capture the source CDC watermark (SCN) ONCE at fresh-run start, for
            # downstream CDC catch-up. Best-effort: a missing source connection or
            # privilege must NEVER block the load.
            import json as _json
            import watermark as _watermark
            _wm = None
            try:
                from source_dialect import get_dialect
                _wdialect = get_dialect()
                _wconn = _wdialect.get_connection()
                try:
                    _wm = _wdialect.capture_watermark(_wconn)
                finally:
                    _wconn.close()
            except Exception as _werr:
                logging.warning('Source watermark capture skipped (no source connection?): %s', _werr)
            # Prefer the SCN captured at split time (before boundary discovery) when its
            # queryfile matches — narrower CDC loss window. Else fall back to load-start.
            _split_sidecar = None
            try:
                _sw_path = os.path.join(os.path.dirname(config.LOG_OUTPUT_PATH) or '.', 'split_watermark.json')
                with open(_sw_path) as _swf:
                    _split_sidecar = _json.load(_swf)
            except (OSError, ValueError):
                _split_sidecar = None
            _wm, _used_split = _watermark.choose_watermark(_split_sidecar, _wm, config.QUERY_FILE)
            if _used_split:
                print('[watermark] reusing split-time SCN (narrower CDC loss window).')
            else:
                logging.warning('Using load-start SCN (no matching split sidecar) -- CDC loss window is wider.')
            _wsrc = getattr(config, 'ORACLE_DSN', '') or getattr(config, 'ORACLE_HOST', '')
            _wpath = _watermark.capture_and_log(_wm, config.UNIQUE_RUN_ID, _wsrc, config.LOG_OUTPUT_PATH)
            if _wpath and _wm:
                print(f"Initial Load start watermark [{_wm.get('label')}] = {_wm.get('value')}  (logged + sidecar {_wpath})")
            else:
                print('WARNING: source CDC watermark not captured -- set the CDC start point manually (see log).')

        continueRun = True

        # If no more results...
        if len(query_results) == 0:
            continueRun = False

        logging.info('Logging Enabled. Storing at: ' + log_output_path)
        print('Logging Enabled. Storing at: ' + log_output_path)

        try:
            while(continueRun):
                print('Executing at', str(datetime.datetime.now()))
                logging.info('Executing at ' + str(datetime.datetime.now()))

                runReview()

                # Stall-detect: a slice RUNNING longer than SLICE_MAX_RUNTIME_SECONDS
                # (0 = disabled) never enters DONE_STATUSES and hangs the loop. Force it
                # FAILED so the end-of-run gate surfaces it loudly.
                import run_safety as _run_safety
                _now = datetime.datetime.now()
                _max_rt = getattr(config, 'SLICE_MAX_RUNTIME_SECONDS', 0)
                for _qry in [q for q in query_results if q.status in config.RUNNING_STATUSES]:
                    if _run_safety.is_stalled(_qry.started_datetime, _now, _max_rt):
                        _qry.status = 'FAILED'
                        _qry.notes = (_qry.notes or '') + ' [stalled > ' + str(_max_rt) + 's]'
                        _qry.finished_datetime = _now
                        try:
                            update_record(_qry, True)
                            print('[stall] slice #' + str(_qry.roworder) + ' marked FAILED (running > ' + str(_max_rt) + 's)')
                        except Exception as _se:
                            logging.warning('[stall] failed to persist stall-FAILED for slice #%s: %s', _qry.roworder, _se)
                            _qry.status = 'RUNNING'
                            _qry.notes = None
                            _qry.finished_datetime = None

                time.sleep(polling_interval_seconds)

                continueRun = False

                # If there are any not completed, we still continue
                for qry in [qry for qry in query_results if qry.status not in config.DONE_STATUSES]:
                    continueRun = True

            # End-of-run completeness gate. Re-read the CURRENT rows from the backend
            # (don't trust the last in-memory poll) and judge with reconcile.final_verdict.
            import sys
            import reconcile
            import run_safety as _run_safety

            _final_rows = update_and_get_current_status()
            _verdict = reconcile.final_verdict(_final_rows)
            if _verdict != 'ALL_COMPLETE':
                _off = reconcile.offending_counts(_final_rows)
                _only_faileddrop = set(_off.keys()) <= {'COMPLETED-FAILEDDROP'}
                if _only_faileddrop:
                    # Data transferred; only post-transfer cleanup (undeploy/drop) failed.
                    # Clear the run so the next invocation doesn't loop forever on rows
                    # that can never reach COMPLETED. Operator must clean up apps manually.
                    banner = (
                        '\n' + '!' * 64 + '\n'
                        + ' RUN ' + str(config.UNIQUE_RUN_ID) + ' COMPLETED WITH CLEANUP FAILURES: ' + _verdict + '\n'
                        + ' COMPLETED-FAILEDDROP slices (data transferred, cleanup failed): ' + str(_off) + '\n'
                        + ' Run cleared. Manually undeploy/drop the listed apps and namespaces.\n'
                        + '!' * 64
                    )
                    logging.warning(banner)
                    print(banner)
                    clear_runid(config.UNIQUE_RUN_ID)
                    _run_safety.delete_marker(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
                    _lock_released = True
                    _run_safety.release_lock(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
                    sys.exit(1)
                banner = (
                    '\n' + '!' * 64 + '\n'
                    + ' RUN ' + str(config.UNIQUE_RUN_ID) + ' DID NOT COMPLETE CLEANLY: ' + _verdict + '\n'
                    + ' non-COMPLETED slices: ' + str(_off) + '\n'
                    + ' Run NOT cleared (iscurrentrow kept). Inspect, then reset / reconcile / resume.\n'
                    + '!' * 64
                )
                logging.error(banner)
                print(banner)
                _lock_released = True
                _run_safety.release_lock(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
                sys.exit(1)

            runMessage = (
                'Run completed: all ' + str(len(_final_rows)) + ' slices COMPLETED at '
                + str(datetime.datetime.now())
                + ' -- run `python manage.py reconcile` to confirm row counts.'
            )
            clear_runid(config.UNIQUE_RUN_ID)
            _run_safety.delete_marker(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
            _lock_released = True
            _run_safety.release_lock(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
            logging.info(runMessage)
            print(runMessage)
        finally:
            if not _lock_released:
                import run_safety as _run_safety
                _run_safety.release_lock(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
