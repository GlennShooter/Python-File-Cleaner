#Import relevant libraries
import sys
import json
import csv
import re
import collections

def readConfig(file): 
	""" Read the configeration file and store key variables needed in other functions also copies date from the read file to the write file"""

	with open(file) as jconfig:
# Global variables which are needed in later functions are parsed from the configeration file and stored
		global config
		global infile
		global file_sep
		global file_format
		global nulls
		global metafile
		global outputfile
		global fill_nulls
		global fill_values
		global file_normalise 
		global file_sort
		global file_valid
 
		config = json.load(jconfig)
		infile = str(config["inputfile"])
		file_sep = str(config["separator"])
		file_format = str(config["format"])
		drop_null = config["preprocess"]["missing"]["dropnull"]
		metafile = str(config["metafile"])
		outputfile = str(config["outputfile"])
		nulls = [str(i) for i in drop_null]
		fill_null = config["preprocess"]["missing"]["fillnull"]  
		fill_null_keys =[] 
		fill_null_values =[] 
		for key,value in fill_null.items():
			fill_null_keys.append(key)
			fill_null_values.append(value)
		fill_nulls =[str(i) for i in fill_null_keys]
		fill_values = [str(i) for i in fill_null_values]
		normal = config["preprocess"]["normalise"]
		file_normalise = [str(i) for i in normal]
		file_sort = config["preprocess"]["sorting"]
		file_valid = config["preprocess"]["validate_card"]

#I want to copy the content of the sample data file to the proccess data file for later in the program so as i edit the data the final file is edited rather than rewritten
		if file_format == "tabular":
			with open(infile) as file:
				reader = csv.reader(file)
				with open (outputfile,"w") as edit:
					writer = csv.writer(edit)
					for row in reader:
						writer.writerow(row)
					file.close()
					edit.close()
		
		if file_format == "json":
			with open(infile) as file:
				j_file = json.load(file)
				with open(outputfile,"w") as edit:
					json.dump(j_file,edit, indent = 4)
					file.close()
					edit.close()
					
def readData():
	""" Opens the read file and counts the number of columns and rows, these will be used later to build the metadata"""
	global columns 
	global rows 
	if file_format == "tabular":
		with open(infile) as file:
			reader = csv.reader(file)
			top_row = []
			rows = 0
			columns = 0
			
			for row in reader:
				for i in row:
					columns += 1
				break
			for row in reader:
				rows += 1
	
	if file_format == "json":
		with open(infile) as file:
			j_file = json.load(file)
			rows = 0
			columns = 0
			for row in j_file:
				for key in row:
					columns += 1
				break 
			for row in j_file:
				rows += 1
				
def generateMetadata():
	"""Builds a metadata dictionary then dumps the dictionary into the metadata.json file"""
#As building the metadata is different depending on the file format i have two if statements one for tabular csv data and one for json data
	if file_format == "tabular":
		first_row = [] 
		vars = [] 
		with open(infile) as file:
			reader = csv.reader(file)
			for row in reader:
				first_row.append(row)
				break
			for row in first_row:
				for x in range(len(row)):
					vars.append(row[x])	
		with open (infile) as file:
			reader = csv.DictReader(file, delimiter = file_sep)
			data = []
			for row in reader:
				data.append(row)
			
			meta_dict = []			
# This codes purpose is to build dictionaries about each variable, these dictionaries will be used to create the metadata
			for x in range(len(vars)):
				unique_set = set()
				numeric_list = []
				dict = {}
				max = 0
				min = 1000000
				
# This is a regular expression used to distingush between string and numeric data types.
				if re.search("^\d", data[x][vars[x]]):
					type = "numerical"
				else:
					type = "string"
					
				for row in data:
					unique_set.add(row[vars[x]])
					numeric_list.append(row[vars[x]])
#once a columns data type has been decided a dictionary will be made giving some details about theat column as the details are different depending on the data type i've used if statements			
				if type == "numerical":
					for y in range(len(numeric_list)):
						try:
							if eval(numeric_list[y]) > max:
								max = eval(numeric_list[y])
						except SyntaxError:
							max = max
							min = min
						except NameError:
							max = max
						try:
							if eval(numeric_list[y]) < min:
								min = eval(numeric_list[y])
						except SyntaxError:
							max = max
							min = min
						except NameError:
							min = min
						dict = {"name" : vars[x], "type" : type, "min": min, "max": max}  
				
				if type == "string":
					dict = {"name" : vars[x], "type" : type, "unique values": len(unique_set)}
# Each dictionary is put into a list this list will be used to create the metadata file if the read file is csv.				
				meta_dict.append(dict)
		
		
		
		
#If the read files format is json then this if statement will run it is very similar to the if statement for the csv file, major differences have been commented on 		
	if file_format == "json":
		with open(infile) as file:
			j_file = json.load(file)
			vars = []
			data = []
			meta_dict = []
			for row in j_file:
				for key in row:
					vars.append(key)
				break
			for row in j_file:
				data.append(row)
			
			for x in range(len(vars)):
				unique_set = set()
				numeric_list = []
				dict = {}
				max = 0
				min = 1000000
				type = ""
				
# This is a regular expression used to distingush between string and numeric data types, it is in a try loop because a TypeError will occure if it is presented with numeric data
# this means the regular expression can distinguish numeric data and string data and if there is a TypeError then the data is numeric.
				try:
					if re.search("^\d", data[x][vars[x]]):
						type = "numerical" 
					else:
						type = "string"
				except TypeError:
					type = "numerical"
					
				for row in data:
					unique_set.add(row[vars[x]])
					numeric_list.append(row[vars[x]])
				
				if type == "numerical":
					for y in range(len(numeric_list)):
						if numeric_list[y] > max:
							max = numeric_list[y]
						
						if numeric_list[y] < min:
							min = numeric_list[y]
						
					dict = {"name" : vars[x], "type" : type, "min": min, "max": max}   
				
				if type == "string":
					dict = {"name" : vars[x], "type" : type, "unique values": len(unique_set)}
				
				meta_dict.append(dict)
#This block of code is used to build the metadata into an ordered dictionary so it has a constant format, then it is wrote to the metadata file.		
	metadata = collections.OrderedDict()
	metadata["filename"] = infile 
	metadata ["format"] = {"type" : file_format, "sep" : file_sep}
	metadata ["numentries"] = rows
	metadata ["numfields"] = columns
	metadata ["fields"] = [meta_dict]
	
	with open(metafile, "w") as meta:
		json.dump(metadata, meta, indent = 4)
					
def dropnull(fieldname):
	""" Searches for null values in a specified column and if a null is found the row is removed"""
	counter = 0
	done = False
# This block of code drops the null values for csv files 	
	if file_format == "tabular":
		while done == False:
			new_row = []
			with open(outputfile) as file:
				reader = csv.reader(file, delimiter = ",")
				for row in reader:
					fieldname_index = row.index(fieldname[counter]) #locates the location of the fieldname in the list and stores the index in the fieldname_index variable 
					break #As i only want the column names and these are stored in the first row a break has been used 
				file.seek(0)
#This block of code is used to find the null values, if a null value is pressent the row will not be added to the new_row list and will not be wrote to the write file. 
				for row in reader:
					if row[fieldname_index] == "null":
						row[fieldname_index] = "NaN"
					if not row[fieldname_index] == "NaN":
						new_row.append(row)
				#file.close()
			with open(outputfile,"wb") as edit:
				writer = csv.writer(edit)
				edit.seek(0)
				for row in new_row:
					writer.writerow(row)
				#edit.close()
#As multiple columns may want their null values dropping this code allows the program to go through each fieldname entered into the function				
			counter += 1
			try: 
				fieldname[counter]
			except IndexError:
				done = True
				
#This block of code drops null values for Json files, it is similar to the proccess used for csv files. 			
	if file_format == "json":
		while done == False:
			new_row = []
			with open(outputfile, "r") as file:
				j_file = json.load(file) 
				for row in j_file:
					if row[fieldname[counter]] == "null":
						row[fieldname[counter]] = "NaN"
					if not row[fieldname[counter]] == "NaN":
						new_row.append(row)
			with open(outputfile, "w") as edit:
				json.dump(new_row, edit, indent=4)
						
			counter += 1
			try: 
				fieldname[counter]
			except IndexError:
				done = True
						

def fillnull(fieldname,value ): 
	""" Finds null values for a fieldname and replaces the null value with a different value"""
	counter = 0
	done = False
# This block of code fills the null values for csv files	
	if file_format == "tabular":
		while done == False:
			new_row = []
			with open(outputfile) as file:
				reader = csv.reader(file, delimiter = ",")
				for row in reader:
					fieldname_index = row.index(fieldname[counter]) #locates the location of the fieldname in the list
					break
				file.seek(0)
				for row in reader:
					if row[fieldname_index] == "null":
						row[fieldname_index] = "NaN"
					if row[fieldname_index] == "NaN":
						row[fieldname_index] = value[counter]
					new_row.append(row)
				file.close()
			with open(outputfile,"wb") as edit:
				writer = csv.writer(edit)
				edit.seek(0)
				for row in new_row:
					writer.writerow(row)
				edit.close()
				
			counter += 1
			try: 
				fieldname[counter]
			except IndexError:
				done = True
#This block of code fills null values for Json files 			
	if file_format == "json":
		while done == False:
			new_row = []
			with open(outputfile) as file:
				j_file = json.load(file)
				for row in j_file:
					if row[fieldname[counter]] == "null":
						row[fieldname[counter]] = "NaN"	
					if row[fieldname[counter]] == "NaN":
						row[fieldname[counter]] = value[counter]
					new_row.append(row)
			with open(outputfile,"w") as edit:
				json.dump(new_row, edit, indent = 4)
				counter += 1
				try: 
					fieldname[counter]
				except IndexError:
					done = True

def normalise(fieldname): 
	""" Normalises a field in range 0-1"""
	counter = 0
	done = False
# This if statement runs if the input file is a csv file 
	if file_format == "tabular":
		while done == False:
			val_list = []
			new_row = []
			with open(outputfile)as file:
				reader = csv.reader(file)
				for row in reader:
					fieldname_index = row.index(fieldname[counter]) #locates the location of the fieldname in the list and stores its index
					break
#This next block of code puts all the values for the fieldname in a list so that the highest and lowest values can be stored 
				for row in reader:
					if row[fieldname_index] == "null":
						row[fieldname_index] = "NaN"
					if row[fieldname_index] == "NaN":
						pass
					else:
						row[fieldname_index] = float(row[fieldname_index])
						val_list.append(row[fieldname_index]) 
				highest = max(val_list)
				lowest = min(val_list)
#Returns the file to the begining and appends the column names to my new_row variable				
				file.seek(0)
				for row in reader:
					new_row.append(row)
					break
#Checks that a value is present in row and then normalises the data, finally the normalised data is stored in the new_row variable 				
				for row in reader:
					if row[fieldname_index] == "null":
						row[fieldname_index] = "NaN"
					if row[fieldname_index] == "NaN":
						pass
					else:
						row[fieldname_index] = float(row[fieldname_index])
						row[fieldname_index] = (row[fieldname_index] - lowest) / (highest - lowest)
					new_row.append(row)
#Writes the normalised data stored in new_row into the output file given to the program by the config file 					
			with open(outputfile,"wb") as edit:
				writer = csv.writer(edit)
				for row in new_row:
					writer.writerow(row)
					edit.close
# If more than one field requers normalisation this block of code allows for that.					
			counter += 1
			try: 
				fieldname[counter]
			except IndexError:
				done = True

#This if statement runs if the input file is Json format 
	if file_format == "json":
		while done == False:
			val_list = []
			new_row = []
			with open(outputfile) as file:
				j_file = json.load(file)
				for row in j_file:
					val_list.append(row[fieldname[counter]])
				highest =  max(val_list)
				lowest = min(val_list)
				for row in j_file: 
					if row[fieldname[counter]] == "null":
						row[fieldname[counter]] = "NaN"
					if row[fieldname[counter]] == "NaN":
						pass
					else:
#I have added a try loop here, this is because if the field that needs to be normalised has null values an error will be called the try loop prevents the program crashing.
#It also tells the user how to rectify the problem so that they can normalise any numeric field they choose. 
						try:
							row[fieldname[counter]] = (row[fieldname[counter]] - lowest) / float((highest - lowest))
						except TypeError:
							print "A TypeError has occured, the data for",fieldname[counter],"could not be normalised. To fix the problem please remove nulls from",fieldname[counter]
					new_row.append(row)
			with open(outputfile,"w") as edit:
				json.dump(new_row, edit, indent = 4)
				edit.close()
			
			counter += 1
			try:
				fieldname[counter]
			except IndexError:
				done = True


def sortData(fieldname): 
	""" Sorts the data based on a field in the configeration file in either assending or desending order"""
# Sorts the data for a csv file 
	if file_format == "tabular":
		new_row = []
		with open(outputfile) as file:
			reader = csv.reader(file, delimiter = ",")
			for row in reader:
				fieldname_index = row.index(fieldname["field"]) #locates the location of the fieldname in the list
				break
			file.seek(0)
			fieldnames =[]
			for row in reader:
				fieldnames.append(row)
				break
			for row in reader:
				new_row.append(row)
			file.seek(0)
			file.next()
			if fieldname["order"] == "desc":
				new_row.sort(key = lambda x : x[fieldname_index], reverse = True)
			else:
				new_row.sort(key = lambda x : x[fieldname_index])
		with open(outputfile,"wb") as edit:
			writer = csv.writer(edit, delimiter = file_sep)
			edit.seek(0)
			final_list = fieldnames + new_row
			for row in final_list:
				writer.writerow(row)
			edit.close()
			
		
# This block of code sorts the data in a json file
	if file_format == "json":
		new_row = []
		with open(outputfile) as file:
			j_file = json.load(file)
			if fieldname["order"] == "desc":
				j_file.sort(key = lambda x : x[fieldname["field"]], reverse = True)
			else:
				j_file.sort(key = lambda x : x[fieldname["field"]])
		
		with open(outputfile, "w") as edit:
			json.dump(j_file, edit, indent = 4) 
			edit.close()
	
def validateDebitCard():
	"""Validates debitcard data based on the criteria given in the project outline"""
#For this code to run the configeration file has to specify that the debitcard data is supposed to get validated 
	if file_valid == True: 
#Different code will be ran based on if the file_format is csv or json 
		if file_format == "tabular":
			first_row = []
			variables = []
			valid_row = []
		 
			with open(outputfile) as file:
				reader = csv.DictReader(file)
				data = []
				header = []
				valid_row = []
				final_row = []
				for row in reader:
					data.append(row)
# Any value that is seperated by - is checked to ensure it is of a valid form.
				for row in data:
					if re.search("[ -]+",row["debitcard"]):
						if re.search("(\d\d\d\d)-(\d\d\d\d)-(\d\d\d\d)-(\d\d\d\d)", row["debitcard"]):
							data.remove(row)
# All - are removed and so are spaces so the debit card number is now one number, this makes it easier to validate 
				for row in data:
					if re.search("[ -]+",row["debitcard"]):
						row["debitcard"] = row["debitcard"].replace("-","")
						if re.search("[ ]+",row["debitcard"]):
							row["debitcard"] = row["debitcard"].replace(" ","")
#The first number of each debit card is validated
				for row in data:
					if not re.search("^[4-6]", row["debitcard"]):
						data.remove(row)
#The length of card number is validated 
				for row in data:
					if not re.search("^(\d{16})$", row["debitcard"]):
						data.remove(row)
#The card number is checked to ensure no number is repeated atleast 4 time 
				for row in data:
					if re.search("(\d)\1{3}", row["debitcard"]):
						data.remove(row)
				
				for row in data:
					for key in row.keys():
						header.append(key)
					break 
				
		
			with open(outputfile,"wb") as edit:
				writer = csv.writer(edit)
				writer.writerow(header)
				for row in data:
					valid_row = []
					for key,value in row.items():
						valid_row.append(value)
					writer.writerow(valid_row)
					
					
		if file_format == "json":
			with open(outputfile) as file:
				j_file = json.load(file)
				data = []
				valid_row =[]
				for row in j_file:
					row["debitcard"] = str(row["debitcard"])
					data.append(row)
# Any value that is seperated by - is checked to ensure it is of a valid form.
				for row in data: 
					if re.search("[ -]+",row["debitcard"]):
						if re.search("(\d\d\d\d)-(\d\d\d\d)-(\d\d\d\d)-(\d\d\d\d)", row["debitcard"]):
							data.remove(row)
						
				for row in data: 
					if re.search("[ -]+",row["debitcard"]):
						row["debitcard"] = row["debitcard"].replace("-","")
						if re.search("[ ]+",row["debitcard"]):
							row["debitcard"] = row["debitcard"].replace(" ","")
				for row in data:
	#Checks to see if the card number starts with a number between 4 and 6				
					if not re.search("^[4-6]", row["debitcard"]):
						data.remove(row)
			
				for row in data:
	#Checks to see if the card number is only 16 numbers in length, if it is not all - and spaces are removed and the number is checked again
					if not re.search("^(\d{16})$", row["debitcard"]):
						data.remove(row)
					
				for row in data:			
	#Checks to see if the card number has 4 consecutive repeated digits 
					if re.search("(\d)\1{3}", row["debitcard"]):
						data.remove(row)
			
			with open(outputfile, "w") as edit:
				json.dump(data,edit, indent = 4)

def preprocessData():
	"""Gives the functions needed to preprocess the data the values needed to work"""
	dropnull(nulls)
	fillnull(fill_nulls,fill_values)
	normalise(file_normalise)
	sortData (file_sort)
	validateDebitCard()
     
def main(configFile):
	"""Calls the functions needed to write the metadata and preprocess the data"""
	readConfig(configFile)
	readData()
	generateMetadata()
	preprocessData()
   
 

if __name__ == '__main__':
    if(len(sys.argv)>1):
        configFile = sys.argv[1]
    else:
        configFile = "config.json"
    main(configFile)
