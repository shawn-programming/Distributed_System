from mrjob.job import MRJob
from mrjob.step import MRStep

class Condorcet1(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        oneLine = line.split(',')
        
        mapledData = list()
        for i in range(len(oneLine)-1):
            for j in range(i+1, len(oneLine)):
                if oneLine[i] < oneLine[j]:
                    temp = ((oneLine[i], oneLine[j]), "1")
                    mapledData.append(temp)
                else:
                    temp = ((oneLine[j], oneLine[i]), "0")
                    mapledData.append(temp)
        yield mapledData


    def reducer(self, key, values):
        keyA = key[0]
        keyB = key[1]

        count = {keyA : 0, keyB : 0}

        for v in values:
            if v == "1":
                count[keyA] += 1
            else:
                count[keyB] += 1
        
        if count[keyA] > count[keyB]:
            yield (keyA, keyB)
        else:
            yield (keyB, keyA)

if __name__ == '__main__':
    Condorcet1.run()
