def iterable_to_scala_list(jvm, iterable):
    return jvm.scala.collection.JavaConversions.\
        iterableAsScalaIterable(iterable).\
        toList()

def iterable_to_scala_set(jvm, iterable):
    return jvm.scala.collection.JavaConversions.\
        iterableAsScalaIterable(iterable).\
        toSet()

def simple_date_format(jvm, s):
    return jvm.java.text.SimpleDateFormat(s)

def tuple2(jvm, t):
    return jvm.scala.Tuple2(*t)
