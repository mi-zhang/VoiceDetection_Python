#takes a list of 3 dimensional features
#loaded from a csv file at this stage, but JSON can be easily included
 
#Does voice classification. Steps are
#	- Loads the voicing parameters from a JSON file. This file can be updated anytime
#	- Computes the mvnpdf for all the feature values
#	- does a viterbi on top of that

import csv
import numpy
import math
from pylab import *

#load the csv file and load the data
def loadFeaturesFromCSV(csvFileName,inferenceCsvFileName):
    #featureData = csv.reader(open(csvFileName, 'r'));
    featureData = genfromtxt(csvFileName, delimiter=',');
    inferenceData = genfromtxt(inferenceCsvFileName, delimiter=',');	
    return featureData,inferenceData



#computes viterbi path	
def computeViterbiPath(featureVector,inferenceData):
    #load the voicing parameters
    #features list: 
    #	1) non-initial auto-correlation peak, 
    #	2) number of peaks
    #	3) relative spectral entropy		
    #mean_unvoiced = mat([ 0.3226, 9.1607, 0.2024])
    #cov_unvoiced = mat([[0.0205,-1.2516,-0.0057],[-1.2516,154.9886,0.6864],[-0.0057,0.6864,0.0129]])
    #mean_voiced = mat([ 0.5003, 4.9901, 0.4280])
    #cov_voiced = mat([[0.0628,-1.2244,0.0257],[-1.2244,55.6850,-0.5305],[0.0257,-0.5305,0.0283]])
    #transitionMatrix = mat([[0.9745,0.0255] , [0.0584,0.9416]])
    #mean_unvoiced = mat([0.2826,10.4119,0.2277])
    #cov_unvoiced = mat([[0.0106,-0.5600,-0.0007],[-0.5600,107.1727,0.3140],[-0.0007,0.3140,0.0056]])
    #mean_voiced = mat([ 0.5149,7.7064,0.4575])
    #cov_voiced = mat([[0.0351,-0.3521,0.0163],[-0.3521,28.8938,0.1430],[0.0163,0.1430,0.0263]])
    #transitionMatrix = mat([[0.9868,0.0132] , [0.0547,0.9453]])
    mean_unvoiced = mat([ 0.2768, 11.1210, 0.1827])
    cov_unvoiced = mat([[0.0097, -0.5698, -0.0020],[-0.5698, 118.9467, 0.4676],[-0.0020, 0.4676, 0.0057]])
    mean_voiced = mat([ 0.4739, 8.5085, 0.3860])
    cov_voiced = mat([[0.0364, -0.4985, 0.0051],[-0.4985, 36.4117, 0.2136],[0.0051, 0.2136, 0.0200]])
    transitionMatrix = mat([[0.9869,0.0131] , [0.0536,0.9464]])	
    prior = mat([0.5,0.5])    
	
    #invoke gaussian observation probability vector
    unvoicedEmissionProb, voicedEmissionProb = computeGaussianObservationProbability(featureVector,mean_unvoiced,cov_unvoiced,mean_voiced,cov_voiced)
    
    	
    #do viterbi algorithm here	
    sizeFeatureVector = shape(featureVector)
    noOfDataPoints = sizeFeatureVector[0]    
    
    #total number of data points	
    T = noOfDataPoints
    #total number of classes
    M = 2	
    #Debug:  T = 150
    	
    #initialize Dmax array    
    Dmax = zeros((T+1,M))
    
    #initialize Dmax
    for iClasses in range(M):
        Dmax[0,iClasses] = 0	
    
    
    #initialize d
    d = zeros([M,M])
    			
    	
    #emission probabilities	
    emissionProb = matrix(array([unvoicedEmissionProb.transpose(), voicedEmissionProb.transpose()]))
    #Debug: print type(emissionProb), shape(emissionProb)
    #Debug: print emissionProb	
    	
    #initialize d
    for j in range(M):	#for \omega_{i_k}
        for k in range(M): #for \omega_{i_{k-1}}
            #Debug: print numpy.log(prior[0,j]*emissionProb[j,0])		
            d[j,k] = numpy.log(prior[0,j]*emissionProb[j,0])
    			
    #implemented from "Pattern Recognition, Fourth Edition" by Sergios Theodoridis, Konstantinos Koutroumbas	
    maximalPath = zeros([T,M])	
    for t in range(T):
        for j in range(M):	#for \omega_{i_k}
            tempDmax = zeros([M,1])		
            for k in range(M): #for \omega_{i_{k-1}}
                tempDmax[k,0] = d[j,k] + Dmax[t,k]				
                                
                #set value after
                #Debug: print emissionProb[j,t], transitionMatrix[j,k], transitionMatrix[j,k]*emissionProb[j,t]  			
                d[j,k] = numpy.log(transitionMatrix[j,k]) + numpy.log(emissionProb[j,t])
            
            #find from the maximum Dmax
            Dmax[t+1,j] = tempDmax.max()			
            occurences = numpy.where(tempDmax == tempDmax.max())
            #Debug: print type(occurences), shape(occurences), occurences,	occurences[0], occurences[0][0]
            maximalPath[t,j] = occurences[0][0] 			
            #Debug: break 
        #Debug: break 		
        		
    #Find best path
    viterbiPath = zeros([T,1])
    #Debug: print maximalPath
    #Debug: print Dmax[T]
    	
    tempDmax = Dmax[T]	
    #print tempDmax, numpy.where(tempDmax == tempDmax.max())[0][0]
    viterbiPath[T-1,0] =  numpy.where(tempDmax == tempDmax.max())[0][0]	
    for t in range(T-2):
        viterbiPath[T-2-t,0] = maximalPath[T-2-t,viterbiPath[T-1-t,0]]
    
    #print viterbiPath, mat(inferenceData).transpose()[0:150]		
     
	#plot the values
    subplot(211)
    plot(viterbiPath)	
    axis('tight')
    ylim((-0.1,1.1))	
    subplot(212)
    plot(inferenceData)
    axis('tight')	
    ylim((-0.1,1.1))	
    show()	
    	
    pass
    
#computes the gaussioan probabilities	
def computeGaussianObservationProbability(featureVector,mean_unvoiced,cov_unvoiced,mean_voiced,cov_voiced):
    #http://en.wikipedia.org/wiki/Multivariate_normal_distribution
    k = 3.0
    inv_voicedCov = linalg.inv(cov_voiced);
    inv_unvoicedCov = linalg.inv(cov_unvoiced);
    
    #Debug: print cov_unvoiced
    #Debug: print inv_unvoicedCov
    #Debug: print cov_voiced
    #Debug: print inv_voicedCov	
    	
    det_voicedCov = numpy.linalg.det(cov_voiced);
    det_unvoicedCov = numpy.linalg.det(cov_unvoiced);
    
    sizeFeatureVector = shape(featureVector)
    noOfDataPoints = sizeFeatureVector[0]	
   	
    unvoicedEmissionProb = mat(numpy.zeros((noOfDataPoints,1)))
    voicedEmissionProb = mat(numpy.zeros((noOfDataPoints,1)))
    
    print pow((2*numpy.pi),k/2), pow(math.fabs(det_unvoicedCov),0.5), det_unvoicedCov	
    print ((pow((2*numpy.pi),k/2)*pow(math.fabs(det_unvoicedCov),0.5))),((pow((2*numpy.pi),k/2)*pow(math.fabs(det_voicedCov),0.5))) 			
    	
    j = 0;	
    for featVec in featureVector:
        featVec = mat(featVec)
        unvoicedEmissionProb[j,0] = (1/(pow((2*numpy.pi),k/2)*pow(math.fabs(det_unvoicedCov),0.5)))*math.exp(-0.5*((featVec-mean_unvoiced)*inv_unvoicedCov)*(featVec-mean_unvoiced).transpose())
        voicedEmissionProb[j,0] = (1/(pow((2*numpy.pi),k/2)*pow(math.fabs(det_voicedCov),0.5)))*math.exp(-0.5*((featVec-mean_voiced)*inv_voicedCov)*(featVec-mean_voiced).transpose())
        j = j + 1		
        #print unvoicedEmissionProb[j,0], voicedEmissionProb[j,0]		
    #pass
    
    #plot the values
    #Debug: subplot(211)
    #Debug: plot(unvoicedEmissionProb)	
    #Debug: axis('tight')	
    #Debug: subplot(212)
    #Debug: plot(voicedEmissionProb)
    #Debug: axis('tight')	
    #Debug: show()	
    	
    return unvoicedEmissionProb, voicedEmissionProb	

#the main function
if __name__ == "__main__":
    #load a csv file with three dimensions and inference to compare
    featureData,inferenceData =  loadFeaturesFromCSV('features.csv','inference.csv')   	
    #compute the gaussian observation probabilities
    computeViterbiPath(featureData,inferenceData)
    
    #compute the optimal path with viterbi	