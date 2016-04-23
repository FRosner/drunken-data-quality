def iterableAsScalaList(jvm, iterable):
    return jvm.scala.collection.JavaConversions.\
        iterableAsScalaIterable(iterable).\
        toList()
