

from sqlite3 import *
import os
#import wavWrite
import pdb
import array 
import wave
import hashlib

def to_Short(strr):
    int_rep = 0
    temp = ord(strr[0])
    int_rep = int_rep + temp        
    int_rep = int_rep << 8
    temp = ord(strr[1])
    int_rep = int_rep + temp        
    return int_rep

def to_Int2(strr):
    int_rep = 0
    #print len(strr)	
    for i in range(0,3):
        temp = ord(strr[i])
        int_rep = int_rep + temp        
        int_rep = int_rep << 8
    temp = ord(strr[3])
    int_rep = int_rep + temp
    return int_rep

def to_Int(strr):
    int_rep = 0
    #print "..", len(strr)	
    for i in range(0,4):
        #print i	
        temp = ord(strr[i])
        int_rep = int_rep + temp        
        int_rep = int_rep << 8
    return int_rep

def to_Float(strr):
    int_rep = to_Int2(strr)
    bits = int_rep
    s = -1
    if (bits >> 31) == 0:
        s = 1
    
    e =(bits >> 23) & 0xff
    
    if e == 0:
        m = (bits & 0x7fffff) << 1
    else:
        m = (bits & 0x7fffff) | 0x800000
        
    num = s * m * (2 ** (e-150)) 
    #print num
    x = str(num)
    return x

def to_Long(str):
    int_rep = 0
    for i in range(0,7):
        temp = ord(str[i])
        int_rep = int_rep + temp        
        int_rep = int_rep << 8
    return int_rep

def to_Double(strr):
    int_rep = to_Long(strr)
    bits = int_rep
    s = -1
    if (bits >> 63) == 0:
        s = 1
    
    e =(bits >> 52) & 0x7ffL
    
    if e == 0:
        m = (bits & 0xfffffffffffffL) << 1
    else:
        m = (bits & 0xfffffffffffffL) | 0x10000000000000L
        
    num = s * m * (2 ** (e-1075)) 
    #print num
    x = str(num)
    return x

def convertAudioBlobToShortArray(audio_data):
    audio_short_data = []
    for i in range(0,len(audio_data),2):
        uInt = ord(audio_data[i])<<8 | ord(audio_data[i+1])
        s16 = (uInt + 2**15) % 2**16 - 2**15
        audio_short_data.append(s16)
    return audio_short_data  

def handleDbrFile(filename, filedir):    

    ''' 
    ### sqlite file handling #######
    '''
        
	#     	
    filelocation = filedir + '/' + filename
        
    # create the data directory    
    if os.path.exists( './data' ):
        pass
    else:
        os.makedirs('./data')
            
    # parse file name for MAC address
    splittedParts = filelocation.split('_')[1]            
    macAddress = splittedParts.split('.')[0]
    #DEBUG: web.debug("Mac Address: "    + macAddress)        
    
    # get the timestamp from the file name
    fileTimestamp = filename.split('_')[0]    
    
    # check whether the data directory for the subject exist
    if os.path.exists( './data/' + macAddress ):
        pass
    else:
        os.makedirs('./data/' + macAddress)    

    # create the subdirectories for each sensing modality
    if os.path.exists( './data/' + macAddress + '/dbr'):
        pass
    else:
        ensure_dir('./data/' + macAddress + '/accel/')
        ensure_dir('./data/' + macAddress + '/audio/')
        ensure_dir('./data/' + macAddress + '/loc/')
        ensure_dir('./data/' + macAddress + '/wifi/')
        ensure_dir('./data/' + macAddress + '/ntp/')        
        ensure_dir('./data/' + macAddress + '/dbr/')     
        ensure_dir('./data/' + macAddress + '/img/')
        ensure_dir('./data/' + macAddress + '/audioFeaturesAndInference/') 
        ensure_dir('./data/' + macAddress + '/Conversasion/')  		

    # now working on the database file
    file_name_realative = filename
    audio_file_name = './data/' + macAddress + '/audio/' + file_name_realative[:-3] + 'aud'
    AUDIO_FILE = open(audio_file_name, "w", 0) 
    
    accel_file_name = './data/' + macAddress + '/accel/' + file_name_realative[:-3] + 'accel'
    ACCEL_FILE = open(accel_file_name, "w", 0) 
    
    loc_file_name = './data/' + macAddress + '/loc/' + file_name_realative[:-3] + 'loc'
    LOC_FILE = open(loc_file_name, "w", 0) 
    
    wifi_file_name = './data/' + macAddress + '/wifi/' + file_name_realative[:-3] + 'wifi'
    wifi_FILE = open(wifi_file_name, "w", 0)

    NTP_file_name = './data/' + macAddress + '/ntp/' + file_name_realative[:-3] + 'ntp'
    NTP_FILE = open(NTP_file_name, "w", 0) 
    
    FeaturesAndInference_file_name = './data/' + macAddress + '/audioFeaturesAndInference/' + file_name_realative[:-3] + 'FeatAndInference'
    FeaturesAndInference_FILE = open(FeaturesAndInference_file_name, "w", 0) 
    
    Conversasion_file_name = './data/' + macAddress + '/Conversasion/' + 'all.' + 'conv'
    Conversasion_FILE = open(Conversasion_file_name, "a") 	
        
    #pdb.set_trace()	
    conn2 = connect(filelocation)
    curs = conn2.cursor()    
    curs.execute("select event_id,event_source_id,event_data,event_time,sync_id from events")
    
    audio_data_list = []
        
    for row in curs:
    # for audio data
        timestamp = row[3]
        # if row[1] == 0: # write the audio data
        if row[1] == 112: # write the audio data
            audio_data = row[2]
            audio_data_short_list = convertAudioBlobToShortArray(audio_data)
            AUDIO_FILE.write("" + str(timestamp) + "," + str(row[4]) + "," + str(audio_data_short_list).strip('[]'))			
            AUDIO_FILE.write('\n')
            #pdb.set_trace()			
            audio_data_list.extend(audio_data_short_list)            
        elif row[1] == 1: # writes accel data
            accel_data = row[2]
            ACCEL_FILE.write(to_Double(accel_data[0:7]))
            ACCEL_FILE.write(',')
            ACCEL_FILE.write(to_Double(accel_data[8:15]))
            ACCEL_FILE.write(',')
            ACCEL_FILE.write(to_Double(accel_data[16:23]))
            ACCEL_FILE.write('\n')
        elif row[1] == 2: # writes loc data
            loc_data = row[2]
            LOC_FILE.write(to_Double(loc_data[0:7]))
            LOC_FILE.write(',')
            LOC_FILE.write(to_Double(loc_data[8:15]))
            LOC_FILE.write('\n')
        elif row[1] == 9: # writes wifi data
            wifi_data = row[2]
            wifi_FILE.write(wifi_data)
            wifi_FILE.write('\n')
        elif row[1] == 10: # writes bluetooth data
            pass        
            #bluetooth_data = row[2]
            #blue_FILE.write(bluetooth_data)
            #blue_FILE.write('\n')        
        elif row[1] == 11: # writes NTP data
            NTP_data = row[2]
            NTP_FILE.write(NTP_data)
            NTP_FILE.write('\n')
        elif row[1] == 111: # 31: # writes features
            data = row[2]	
            numAcorrPeaks = int(round(float(to_Float(data[0:4])))) #all other features
            maxAcorrPeakVal = to_Float(data[4:8])
            maxAcorrPeakLag = to_Float(data[8:12])
            spectral_entropy = to_Float(data[12:16])
            rel_spectral_entropy = to_Float(data[16:20])
            energy = to_Float(data[20:24]) #energy probability	
            numberOfInferences = 20
            inferenceArray = []			
            for i in range(0,numberOfInferences):	#only 5 inference were kept		
                inferenceArray.append(ord(data[24 + i]))	
            print "values", numAcorrPeaks, maxAcorrPeakVal, maxAcorrPeakLag, spectral_entropy, rel_spectral_entropy, energy
            print inferenceArray
            emissionProbabilitiesUnvoiced = to_Float(data[44:48]) #emission probability
            emissionProbabilitiesVoiced = to_Float(data[48:52])			
            print emissionProbabilitiesUnvoiced, emissionProbabilitiesVoiced, len(data), len(data[52:55])
            numAcorrPeaks2 = to_Int2(data[52:56])
            print numAcorrPeaks2
            acorrPeakValueArray = []
            index = 56			
            for i in range(0,numAcorrPeaks):		
                acorrPeakValueArray.append(to_Float(data[(index + i*4):(index + 4  + i*4)]))	
            print acorrPeakValueArray				
            index = 56 + numAcorrPeaks*4					
            acorrPeakLagArray = []	#acorrPeakLag		
            for i in range(0,numAcorrPeaks):
                #print i			
                acorrPeakLagArray.append(to_Short(data[(index + i*2):(index + 2 + i*2)]))	
            print str(acorrPeakLagArray) + "\n\n"	
            '''emissionProbabilitiesUnvoiced = to_Double(data[48:55]) #emission probability
            emissionProbabilitiesVoiced = to_Double(data[56:63])			
            inferences = to_Double(data[64:71])
            numberOfInferences = 20
            inferenceArray = []			
            for i in range(numberOfInferences):	#only 5 inference were kept		
                inferenceArray.append(to_Double(data[(64 + i*8):(71 + i*8)]))			
            acorrPeakValueArray = []
            index = 72 + (numberOfInferences-1)*8	# acorrPeakValue
            #pdb.set_trace()				
            for i in range(numAcorrPeaks):		
                acorrPeakValueArray.append(to_Double(data[(index + i*8):(index + 7  + i*8)]))				
            index = 72 + (numberOfInferences-1)*8 + numAcorrPeaks*8 					
            acorrPeakLagArray = []	#acorrPeakLag		
            for i in range(numAcorrPeaks):	
                acorrPeakLagArray.append(to_Double(data[(index + i*8):(index + 7 + i*8)]))				
            #pdb.set_trace()
            #print "acorrPeakLagArray ",acorrPeakLagArray 
            #print "acorrPeakValueArray ",acorrPeakValueArray 			
            #Conversasion_FILE.write('\n')		
            #pass'''
			
			# NOTE: This is what is written in files.
            inferencedata = "" + str(timestamp) + "," + str(row[4]) + "," + str(numAcorrPeaks) + "," + maxAcorrPeakVal + "," + rel_spectral_entropy + "," +  emissionProbabilitiesUnvoiced + "," + emissionProbabilitiesVoiced + "," + str(inferenceArray)[1:-1].replace(" ", "") + "," + str(acorrPeakValueArray)[1:-1].replace("'", "").replace(" ", "") + "," + str(acorrPeakLagArray)[1:-1].replace(" ", "")
            FeaturesAndInference_FILE.write(inferencedata)
            FeaturesAndInference_FILE.write('\n')
        elif row[1] == 24: # writes features
            data = row[2]	
            Feature_data = []		
            #str(to_Long(data[0:7]))+","+str(to_Long(data[8:15]))
            Feature_data_length = len(data)/8 # double points
            numAcorrPeaks = int(round(float(to_Double(data[0:7])))) #all other features
            maxAcorrPeakVal = to_Double(data[8:15])
            maxAcorrPeakLag = to_Double(data[16:23])
            spectral_entropy = to_Double(data[24:31])
            rel_spectral_entropy = to_Double(data[32:39])
            energy = to_Double(data[40:47]) #energy probability		
            emissionProbabilitiesUnvoiced = to_Double(data[48:55]) #emission probability
            emissionProbabilitiesVoiced = to_Double(data[56:63])			
            inferences = to_Double(data[64:71])
            numberOfInferences = 20
            inferenceArray = []			
            for i in range(numberOfInferences):	#only 5 inference were kept		
                inferenceArray.append(to_Double(data[(64 + i*8):(71 + i*8)]))			
            acorrPeakValueArray = []
            index = 72 + (numberOfInferences-1)*8	# acorrPeakValue
            #pdb.set_trace()				
            for i in range(numAcorrPeaks):		
                acorrPeakValueArray.append(to_Double(data[(index + i*8):(index + 7  + i*8)]))				
            index = 72 + (numberOfInferences-1)*8 + numAcorrPeaks*8 					
            acorrPeakLagArray = []	#acorrPeakLag		
            for i in range(numAcorrPeaks):	
                acorrPeakLagArray.append(to_Double(data[(index + i*8):(index + 7 + i*8)]))				
            #pdb.set_trace()
            #print "acorrPeakLagArray ",acorrPeakLagArray 
            #print "acorrPeakValueArray ",acorrPeakValueArray 			
            #Conversasion_FILE.write('\n')		
            #pass
			
			# NOTE: This is what is written in files.
            inferencedata = "" + str(timestamp) + "," + str(row[4]) + "," + str(numAcorrPeaks) + "," + maxAcorrPeakVal + "," + rel_spectral_entropy + "," +  emissionProbabilitiesUnvoiced + "," + emissionProbabilitiesVoiced + "," + inferences
            FeaturesAndInference_FILE.write(inferencedata)
            FeaturesAndInference_FILE.write('\n')			
        elif row[1] == 25: # writes NTP data
            data = row[2]	
            Conversasion_data = str(to_Long(data[0:7]))+","+str(to_Long(data[8:15]))
            Conversasion_FILE.write(Conversasion_data)
            Conversasion_FILE.write('\n')		

    #let us not bother about audio right now    			
    #wavWrite.make_soundfile(audio_data_list,'./data/' + macAddress + '/audio/' + file_name_realative[:-3] + 'wav')
    if not audio_data_list:
        pass #empty list and no audio
    else:		
        wv = wave.open('./data/' + macAddress + '/audio/' + file_name_realative[:-3] + 'wav', 'w')
        data = array.array('h',audio_data_list) #signed short, see docs for more info    
        wv.setparams((1, 2, 8000, 0, "NONE", "Uncompressed"))
        wv.writeframes(data.tostring())
        wv.close()	
    	
    LOC_FILE.close()    
    ACCEL_FILE.close()
    AUDIO_FILE.close()
    wifi_FILE.close()
    NTP_FILE.close()
    Conversasion_FILE.close()	
    FeaturesAndInference_FILE.close()
    curs.close() 
    conn2.close()
        
    #copy the dbr file
    fin = open(filelocation,'rb')    
    fout = open('./data/' + macAddress + '/dbr/' + filename,'wb') # creates the file where the uploaded file should be stored
    fout.write(fin.read()) # writes the uploaded file to the newly created file.
    fout.close() # closes the file, upload complete.
    fin.close()
              
    #remove the files
    #os.remove(filelocation)  	
    	
    return "Successfully copied everything"	
             
def hashExists(md5hash,user_name,dbname,filelocation):
    conn = psycopg2.connect("dbname='"+dbname+"' user='"+ user_name +"' host='localhost' password='shuvaaa406'")	
    cur = conn.cursor()
    cur.execute("""select * from file_hashes where hashvalue ='""" + md5hash +"'")
    #close the insert cursors
    if cur.fetchone() == None:
        cur.execute("""insert into file_hashes (hashvalue , filename) values (""" + "'"+ md5hash +"','" +  filelocation + "')")
        conn.commit()
        cur.close()
        conn.close()
        return False;		
    #pdb.set_trace()	
    cur.close()
    conn.close()
    return True;	
    	    
def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)  

def md5Checksum(filePath):
    fh = open(filePath, 'rb')
    m = hashlib.md5()
    while True:
        data = fh.read(8192)
        if not data:
            break
        m.update(data)
    return m.hexdigest()		

# Program entrance	
if __name__ == "__main__":

    #handleDbrFile('1390441841_355136051983958.dbr', '../../Datasets/rawData/355136051983958/dbr')
    handleDbrFile('1398719349_358239057380463.dbr', './')
	#handleDbrFile('1396493397_358239058727449.dbr', 'C:\Users\Mi Zhang\Desktop\data2')








    


