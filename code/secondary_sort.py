#redefine secondary sort
class MySecondarySort(object):

    def __init__(self, recordOne, recordTwo):
        self.recordOne = recordOne
        self.recordTwo = recordTwo

    def __eq__(self, other):
        if not isinstance(other,MySecondarySort):            
            raise TypeError("can't cmp other type to MySecondarySort!")     
        if self.recordOne == other.recordOne and self.recordTwo == other.recordTwo:            
            return True        
        else:
            return False

    def __cmp__(self, other):
        if self.__eq__(other):            
            return 0        
        elif self.__lt__(other):            
            return -1        
        elif self.__gt__(other):            
            return 1


    def __lt__(self,other):
        if not isinstance(other, MySecondarySort):            
            raise TypeError("can't cmp other type to MySecondarySort!")  
        if self.recordOne < other.recordOne:            
            return True        
        elif self.recordOne == other.recordOne and self.recordTwo < other.recordTwo:            
            return True        
        else:
            return False
    
    def __gt__(self,other):
        if not isinstance(other, MySecondarySort):            
            raise TypeError("can't cmp other type to MySecondarySort!")  
        if self.recordOne > other.recordOne:            
            return True        
        elif self.recordOne == other.recordOne and self.recordTwo > other.recordTwo:            
            return True        
        else:
            return False