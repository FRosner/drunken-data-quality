def iterableAsScalaList(jvm, iterable):
    return jvm.scala.collection.JavaConversions.\
        iterableAsScalaIterable(iterable).\
        toList()

def iterableAsScalaSet(jvm, iterable):
    return jvm.scala.collection.JavaConversions.\
        iterableAsScalaIterable(iterable).\
        toSet()

def simpleDateFormat(jvm, s):
    return jvm.java.text.SimpleDateFormat(s)
