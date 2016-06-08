def iterableToScalaList(jvm, iterable):
    return jvm.scala.collection.JavaConversions.\
        iterableAsScalaIterable(iterable).\
        toList()

def iterableToScalaSet(jvm, iterable):
    return jvm.scala.collection.JavaConversions.\
        iterableAsScalaIterable(iterable).\
        toSet()

def simpleDateFormat(jvm, s):
    return jvm.java.text.SimpleDateFormat(s)

def tuple2(jvm, t):
    return jvm.scala.Tuple2(*t)
