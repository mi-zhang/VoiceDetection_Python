
# create a synthetic 'sine wave' wave file with set frequency and length
# tested with Python25 and Python30  by vegaseat  28dec2008

import math
import wave
import struct
import pdb

def make_soundfile(data, filename="sine_wave1.wav"):
    """
    creates a wave file with "data". Data can come directly from at byte format from the audio data
    """
    frate = 8000  #sampling frequncy. framerate as a float
    #amp = 8000.0     # multiplier for amplitude 
    
    # make a sine list ...
    #sin_list = []
    #for i in range(data_size):
    #    sin_list.append(math.sin(2*math.pi*freq*(i/frate)))
            
    #pdb.set_trace();		
    wav_file = wave.open(filename, "w")
    
    # required parameters ...
    nchannels = 1
    sampwidth = 2
    framerate = int(frate)
    nframes = len(data) #get from the blob data
    comptype = "NONE"
    compname = "not compressed"

    # set all the parameters at once
    wav_file.setparams((nchannels, sampwidth, framerate, nframes, 
        comptype, compname))

    # now write out the file
    print("may take a few seconds ...")
    for s in data:
        # write the audio frames, make sure nframes is correct
        wav_file.writeframes(struct.pack('h', s))
    wav_file.close()
    print( "%s written" % filename)
