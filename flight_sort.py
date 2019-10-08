from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class MapReduce(MRJob):
	def mapper(self, _, line): 
		data = []
		
		reader = csv.reader(line.split(','), delimiter=',', quotechar='"')
		
		for word in reader:
			data.append(word[0])
			
		if len(data) == 3:
			dst = data[0]
			src = data[1]
			try:
				num = int(data[2])
				yield src, (dst, num, 'src-key')
				yield dst, (src, num, 'dst-key')
			except:
				pass
	
	def reducer(self, key, value):
	
		keyToLoc = []
		keyFromLoc = []
		for x in list(value):
			if x[2] == 'src-key':
				keyToLoc.append((key, x[0], x[1]))
			else:
				keyFromLoc.append((x[0], key, x[1]))
		
		if keyToLoc and keyFromLoc:
			for pair1 in keyFromLoc:
				for pair2 in keyToLoc:
					if pair1[1] == pair2[0]:
						yield((pair1[0], pair2[1]), (pair1[2] * pair2[2]))
			
					
	def mapper_2(self, key, value):
		yield(key, value)

	
	def reducer_2(self, key, value):
		value = list(value)
		if value[0] > 0:
			yield(key, sum(value))
		
	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer),
			MRStep(mapper=self.mapper_2, reducer=self.reducer_2)]
			
if __name__ == '__main__':
	MapReduce.run()
