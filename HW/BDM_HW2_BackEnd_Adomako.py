import csv
from mrjob.job import MRJob, MRStep
import mapreduce as mr

class Sales(MRJob):
    def mapper1(self, _,  v1):#k1 not defined becuase it is index-like wrt enumerate reader
        #v1 = v1.split(",")
        pID = v1[3]
        cID = v1[0]
        iCost = v1[4]
        #key, value ... group the cost
        yield (pID, cID), float(iCost) 
    
    def reducer1(self, IDs, iCosts):#KEYS ARE UNIQUE, effectively quantity sum on values
        yield IDs, sum(iCosts)

    def mapper2(self,IDs,iCosts):
        pIDs = IDs[0]
        yield pIDs,(1,iCosts)
        
    def reducer2(self, pIDs, rev):
        #print(type(rev))
        rev = [*rev]
        yield pIDs, (sum(revTotal[0] for revTotal in rev),round(sum(revTotal[1] for revTotal in rev),2))
        #yield pIDs,sum(rev[0]),sum(rev[1])
        #yield gender[int(k4)], max(v4s, key=lambda x:x[1])#can use for loop to run multiple times, if cmd line
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2)
        ]
    

if __name__ == '__main__':
    Sales.run()
