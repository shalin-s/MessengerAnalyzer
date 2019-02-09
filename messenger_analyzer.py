import os
import sys
import subprocess
import traceback
import gzip
import csv
import datetime
import json

#Note: If two different friends or group chats have exactly the same name, that will mess this up.
#This should be uncommon though. Will fix this bug later, by indexing dict by conversation "id" rather than name.

ANALYSIS_TYPES = ["all-info", "friend-ranking", "friends-timing", "group-ranking"] #acceptable analyses

HELP_TEXT = ""
HELP_TEXT += "MESSENGER ANALYSIS HELP:\n\n"
HELP_TEXT += "Arguments:\n--input [facebook download folder]\n"
HELP_TEXT += "--analysis [analysis type]\n"
HELP_TEXT += "--name [your full name on Facebook]\n"
HELP_TEXT += "--time [unix timestamp in ms to start timing analysis, if performing friends-timing analysis]\n"
HELP_TEXT += "--group [name of group, if performing group-ranking analysis]\n"
HELP_TEXT += "\n\n"
HELP_TEXT += "Analysis type options:\n"
HELP_TEXT += (ANALYSIS_TYPES[0] + ": essentially just dumps all basic statistics, sorted in alphabetical order by conversation title.\n")
HELP_TEXT += (ANALYSIS_TYPES[1] + ": ranks all of your friends by how much you've talked to them (measured by the total length in characters of all of your conversations so far), from most to least.\n")
HELP_TEXT += (ANALYSIS_TYPES[2] + ": takes all of your friends (ranked as described previously) and generates a table, by week, of when you talked to whom, and how much.\n")
HELP_TEXT += (ANALYSIS_TYPES[3] + ": ranks all members of a group from most active to least active (measured by the total length in characters of all their contributions so far).\n")

DATA_DOWNLOAD_FOLDER = None
ANALYSIS_TYPE = None

MY_NAME = None
TIME_START = None
GROUP_NAME = None

if (len(sys.argv) == 1):
	print (HELP_TEXT)
	exit(0)

#Read in command line arguments and echo: 
upcomingArg = ""
for i in range(len(sys.argv)):
	arg = sys.argv[i]
	if (i == 0):
		continue
	if (arg == "--help"): #help
		print (HELP_TEXT)
		exit(0)
	if (arg.startswith("--")):
		upcomingArg = arg[2:]
		continue
	if (upcomingArg == "input"): #input
		if (DATA_DOWNLOAD_FOLDER != None):
			raise ValueError ("More than one data folder provided.")
		else:
			DATA_DOWNLOAD_FOLDER = arg
	elif (upcomingArg == "analysis"): #analysis
		if (ANALYSIS_TYPE != None):
			raise ValueError ("More than one analysis type provided.")
		elif (arg not in ANALYSIS_TYPES):
			raise ValueError ("Invalid analysis type: " + str(arg))
		else:
			ANALYSIS_TYPE = arg
	elif (upcomingArg == "name"): #person's name (precisely as entered in Facebook)
		if (MY_NAME != None):
			raise ValueError ("More than one name provided.")
		else:
			MY_NAME = arg
	elif (upcomingArg == "time"): #person's name (precisely as entered in Facebook)
		if (TIME_START != None):
			raise ValueError ("More than one time range provided.")
		elif (ANALYSIS_TYPE != "friends-timing"):
			raise ValueError ("Time start only applicable for friends-timing analysis")
		else:
			t = None
			try:
				t = int(arg)
			except:
				raise ValueError ("Invalid Time: " + arg)
			TIME_START = t
	elif (upcomingArg == "group"): #person's name (precisely as entered in Facebook)
		if (GROUP_NAME != None):
			raise ValueError ("More than one group name provided.")
		elif (ANALYSIS_TYPE != "group-ranking"):
			raise ValueError ("Group name only applicable for group-ranking analysis")
		else:
			GROUP_NAME = arg
	else:
		raise ValueError("Invalid parameter: " + str(arg) + ". Use --help for help.")

if (DATA_DOWNLOAD_FOLDER == None):
	raise ValueError("Need to specify input folder. Use --help for help.")
if (ANALYSIS_TYPE == None):
	raise ValueError("Need to specify analysis type. Use --help for help.")
if (MY_NAME == None):
	raise ValueError("Need to specify your full name. Use --help for help.")

if (ANALYSIS_TYPE == "friends-timing" and TIME_START is None):
	raise ValueError("Need to specify start time for friends-timing analysis.")
if (ANALYSIS_TYPE == "group-ranking" and GROUP_NAME is None):
	raise ValueError("Need to specify group name for group-ranking analysis.")

print ("Configuration successful.")
print ("Input folder: " + str(DATA_DOWNLOAD_FOLDER))
print ("Starting analysis type: " + str(ANALYSIS_TYPE))
if (ANALYSIS_TYPE == "friends-timing"):
	print ("Start time: " + str(TIME_START))
if (ANALYSIS_TYPE == "group-ranking"):
	print ("Group name: " + str(GROUP_NAME) + "(ranking from most to least active)")

#MAIN PROGRAM (now that inputs have been read in):

DATA_FOLDER_BASE_NAME = DATA_DOWNLOAD_FOLDER[(DATA_DOWNLOAD_FOLDER.rfind(os.sep) + 1):]
MESSAGES_FOLDER = os.path.join(DATA_DOWNLOAD_FOLDER, "messages")
MESSAGES_FOLDER = os.path.join(MESSAGES_FOLDER, "inbox")
CHAT_IDS = [chat_dir for chat_dir in os.listdir(MESSAGES_FOLDER)]
CHAT_FOLDERS = [os.path.join(MESSAGES_FOLDER, chat_id) for chat_id in CHAT_IDS]
CHAT_FILES = [os.path.join(chat_folder, "message.json") for chat_folder in CHAT_FOLDERS]

OUTPUT_EXTENSION = ".csv"
def date_time_string ():
	return datetime.datetime.now().strftime("%y%m%d_%H%M%S")
OUTPUT_FILE = "MessengerAnalysis_" + ANALYSIS_TYPE + "_" + date_time_string() + OUTPUT_EXTENSION
writer = open(OUTPUT_FILE, "w")
def output_line (message):
	writer.write (message + "\n")

print ("Will output results to: " + str(OUTPUT_FILE))

ABBREVIATIONS = ["asap", "brb", "ikr", "jk", "kk", "lmao", "lmfao", "lmk", "lol", "np", "nvm", "ofc", "pls", "ppl", "smh", "sry"] #add more

OUTPUT_HEADER_CI = "Conversation Name,Author Name,Message Count,Total Message Length"
OUTPUT_HEADER_FRIEND = "Friend Name,Combined Message Length,My Length Ratio,Combined Message Count,My Message Count Ratio"
OUTPUT_HEADER_GROUP = "Group Name,Author Name,Combined Message Count,Combined Message Length"

#TIME_START = 1535342400000 #August 27 2018, 12:00:00 AM, Eastern Time
MILLIS_PER_WEEK = 604800000
TIME_BUCKET_SIZE = 1 * MILLIS_PER_WEEK # One week
MAX_NUM_BUCKETS = 200 #can be anything known for certain to be greater than the number of buckets.
TIME_END = 1549706578000 #known time beyond which there cannot be any messages, February 9 2019

CONVERSATION_ID_INDEX = 0
CONVERSATION_NAME_INDEX = 1
AUTHOR_ID_INDEX = 2
AUTHOR_NAME_INDEX = 3
TIMESTAMP_INDEX = 5
CONTENT_INDEX = 6

#Data structure: nested dictionaries:
#One dictionary of all the conversations, indexed by CID
#Within that, a dictionary with keys being AID, and values being ConversationInfo
#and (perhaps) the first entry having AID of "0", and values being ConversationInfo for ALL participants
conversation_info = {}

def remove_weird_characters(s):
    i = 0
    while (i < len(s)):
        c = s[i]
        if (0 <= ord(c) and ord(c) < 128):
            i += 1
        else:
            s = s[:i] + s[(i + 1):]
    return s

def is_time_valid (time): #Don't really need this for now
	#time can be anything parseable to int
	#return (int(time) > TIME_START)
	return True

def get_time_bucket(time):
	return ((int(time) - TIME_START) // TIME_BUCKET_SIZE)

def get_time_bucket_floor(time):
	return (TIME_START + (((int(time) - TIME_START) // TIME_BUCKET_SIZE) * TIME_BUCKET_SIZE))

#Records info, within a single conversation, for a single participant.
class ConversationInfo:
	def __init__(self):
		self.c_id = 0
		self.c_name = 0
		self.a_id = 0
		self.a_name = 0
		#self.emoticon_usage = {} #skip this for now
		self.message_count = 0
		self.total_message_length = 0
		self.reaction_count = 0

		self.used_timestamps = set()
		self.time_buckets = [0] * MAX_NUM_BUCKETS
		self.last_timestamp = 0
		#combined in combined_conversation_info

	def add_message (self, fbm):

		if (int(fbm.time) > self.last_timestamp):
			self.last_timestamp = int(fbm.time)

		if (ANALYSIS_TYPE == "friends-timing"):
			#self.time_buckets[get_time_bucket(fbm.time)] += 1 #for message count
			self.time_buckets[get_time_bucket(fbm.time)] += len(fbm.text) #for message length

		content = fbm.content
		text = fbm.text
		self.message_count += 1
		self.total_message_length += len(text)

		'''
		for a in ABBREVIATIONS:
			self.abbreviation_count += text.lower().count(a)
		'''

	def to_csv_line (self):
		iter = [str(self.c_name), str(self.a_name), str(self.message_count), str(self.total_message_length)]
		for i in range (len(iter)):
			iter[i] = remove_weird_characters(iter[i])
		return ",".join(iter)

	def to_csv_line_without_c_name (self):
		iter = [str(self.a_name), str(self.message_count), str(self.total_message_length)]
		for i in range (len(iter)):
			iter[i] = remove_weird_characters(iter[i])
		return ",".join(iter)

#might be for a group or for an individual conversation
class CombinedConversationInfo:

	def __init__(self, cis):
		#cis is dict of ConversationInfos with the same c_id and c_name, should be just two of them, keys are a_ids
		vals = list(cis.values())
		self.c_id = vals[0].c_id
		self.c_name = vals[0].c_name
		self.is_group_cci = (len(vals) > 2)
		#self.emoticon_usage = {} #keys are emoticon names, values are counters #skip this for now
		self.combined_message_count = sum(v.message_count for v in vals)
		self.combined_message_length = sum(v.total_message_length for v in vals)
		self.time_buckets = [0] * MAX_NUM_BUCKETS
		for i in range (MAX_NUM_BUCKETS):
			self.time_buckets[i] = sum(v.time_buckets[i] for v in vals)
		self.last_timestamp = max(v.last_timestamp for v in vals)

		self.last_bucket = 0
		if (ANALYSIS_TYPE == "friends-timing"):
			self.last_bucket = get_time_bucket(self.last_timestamp)

		self.my_count_proportion = 0
		self.my_count_ratio = 0
		self.my_length_proportion = 0
		self.my_length_ratio = 0

		if (MY_NAME not in cis):
			return

		self.my_count_proportion = cis[MY_NAME].message_count / self.combined_message_count
		self.my_length_proportion = cis[MY_NAME].total_message_length / self.combined_message_length

		if (self.my_count_proportion != 1.0):
			self.my_count_ratio = self.my_count_proportion / (1.0 - self.my_count_proportion)
		else:
			self.my_count_ratio = -1 #indicator that it is undefined

		if (self.my_length_proportion != 1.0):
			self.my_length_ratio = self.my_length_proportion / (1.0 - self.my_length_proportion)
		else:
			self.my_length_ratio = -1 #indicator that it is undefined


	def to_csv_line (self):
		iter = [str(self.c_name), str(self.combined_message_length), str(self.my_length_ratio), str(self.combined_message_count), str(self.my_count_ratio)]
		return ",".join(iter)

	def times_csv_header (last_bucket): #prints unix timestamps for starting times
		result = "Conversation ID"
		for i in range(0, last_bucket): #INTENTIONALLY EXCLUDE LAST BUCKET, AS IT MAY BE CUTOFF INCOMPLETELY
			result += ("," + str((TIME_START + (i * TIME_BUCKET_SIZE)) // 1000)) #convert millis to regular Unix timestamp
		return result

	def times_to_csv_line (self, length = None): #length must be less than MAX_NUM_BUCKETS
		if (length == None):
			length = self.last_bucket
		result = str(self.c_id)
		for i in range(0, length): #INTENTIONALLY EXCLUDE LAST BUCKET, AS IT MAY BE CUTOFF INCOMPLETELY
			result += ("," + str(self.time_buckets[i]))
		return result


class FB_Reaction:
	def __init__(self, reaction_data):
		self.reaction = reaction_data["reaction"]
		self.actor = reaction_data["actor"]

class FB_Message:
	def __init__(self, c_id, c_name, message):
		self.c_id = c_name
		self.c_name = c_name
		self.a_id = message["sender_name"]
		self.a_name = message["sender_name"]
		self.time = message["timestamp_ms"]
		self.content = message["content"]
		self.reactions = []
		if ("reactions" in message):
			self.reactions = [FB_Reaction(r) for r in message["reactions"]]
		self.text = remove_weird_characters(self.content)

def output_all_conversation_info_alphabetically(conversation_infos):
	#output_line ("Conversation Data:")
	OUTPUT_HEADER = OUTPUT_HEADER_CI
	output_line (OUTPUT_HEADER)

	#SORT:
	all_conversation_infos = []
	for v in conversation_info.values():
		for ci in v.values():
			all_conversation_infos.append(ci)
	all_conversation_infos.sort(key=lambda ci: (ci.c_id, ci.a_id))

	for ci in all_conversation_infos:
		output_line (ci.to_csv_line())


#From conversation_info, create dictionary with c_ids as keys, indicating for each friend or group I talk to:
#Combined total message length, message count, everything
#the actual work happens in constructor of CombinedConversationInfo
def create_combined_conversation_infos(ci):
	#ci is the main conversation_info variable
	result = {}
	for k,v in ci.items():
		if (len(list(v.values())) == 0):
			continue
		result[k] = CombinedConversationInfo(v)
	return result

def dict_to_values_list (d):
	l = []
	for k,v in d.items():
		l.append(v)
	return l

#rank friends by either combined message length or total messages
#filter from combined info only individual friends (not groups)
def rank_friends (combined_infos_dict):
	combined_infos_list = dict_to_values_list(combined_infos_dict)
	combined_infos_list_individual = [cci for cci in combined_infos_list if (not cci.is_group_cci)]
	combined_infos_list_individual.sort(key=lambda ci: (ci.combined_message_length, ci.c_id), reverse = True)
	return combined_infos_list_individual

def output_ranked_friends_list (friends_list):
	#output_line ("Friends Ranking:")
	OUTPUT_HEADER = OUTPUT_HEADER_FRIEND
	output_line (OUTPUT_HEADER)

	for ci in friends_list:
		output_line (ci.to_csv_line())

def output_ranked_friends_time_buckets (friends_list):
	#output_line ("Friends (ranked by total message length):")
	last_bucket = max(cci.last_bucket for cci in friends_list)
	OUTPUT_HEADER = CombinedConversationInfo.times_csv_header(last_bucket) #make make this static variable of class conversation info
	output_line (OUTPUT_HEADER)
	
	for ci in friends_list:
		output_line (ci.times_to_csv_line(length = last_bucket))

def get_group_rankings (ci, group_name):
	#ci should be conversation_info
	group_cis = None
	try:
		group_cis = ci[str(group_name)]
	except:
		raise ValueError("Error: group \"" + str(group_name) + "\" not found")
	group_cis_list = dict_to_values_list(group_cis)
	group_cis_list.sort(key=lambda c: (c.total_message_length, c.c_id), reverse = True)
	return group_cis_list

def output_group_rankings (group_name, cis):
	#output_line ("Group Rankings for \"" + str(group_name) + "\":")
	OUTPUT_HEADER = OUTPUT_HEADER_GROUP
	output_line (OUTPUT_HEADER)
	for ci in cis:
		output_line (ci.to_csv_line())

for cn in range(len(CHAT_IDS)):
	chat_id = CHAT_IDS[cn]
	chat_data = json.load(open(CHAT_FILES[cn]))
	if (len(chat_data["messages"]) == 0):
		continue
	chat_name = chat_data["title"]
	conversation_info[chat_name] = {}

	counter = 0
	for message in chat_data["messages"]:
		if (not (message["type"] == "Generic")):
			continue
		fbm = FB_Message(chat_id, chat_name, message)
		if (not is_time_valid(fbm.time)):
			continue
		if (fbm.a_id not in conversation_info[fbm.c_id]):
			new_ci = ConversationInfo()
			conversation_info[fbm.c_id][fbm.a_id] = new_ci
			new_ci.c_id = fbm.c_id
			new_ci.c_name = fbm.c_name
			new_ci.a_id = fbm.a_id
			new_ci.a_name = fbm.a_name
		conversation_info[fbm.c_id][fbm.a_id].add_message(fbm)

if (ANALYSIS_TYPE == "all-info"):
	output_all_conversation_info_alphabetically(conversation_info)

elif (ANALYSIS_TYPE == "friend-ranking" or ANALYSIS_TYPE == "friends-timing"):
	combined_conversation_info = create_combined_conversation_infos(conversation_info)
	friends_list_ranked = rank_friends(combined_conversation_info)

	if (ANALYSIS_TYPE == "friend-ranking"):
		output_ranked_friends_list(friends_list_ranked)

	elif (ANALYSIS_TYPE == "friends-timing"):
		output_ranked_friends_time_buckets(friends_list_ranked)

elif (ANALYSIS_TYPE == "group-ranking"):
	group_rankings = get_group_rankings(conversation_info, GROUP_NAME)
	output_group_rankings (GROUP_NAME, group_rankings)

else:
	raise ValueError ("Invalid analysis type: " + str(arg))

print ("Done")
