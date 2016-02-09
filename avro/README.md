

# Avro Demo
This is some code taken from [the Apache Avro website](http://avro.apache.org/docs/current/gettingstartedjava.html) for basic examples of working with Avro in Java and Python.


----------

Java
-----

Download the latest avro-tools jar file from the Apache Avro Download page: http://www.apache.org/dyn/closer.cgi/avro/

`$ wget [jarfile]`

Using the avro tools jar file, compile the user.avsc file

`$ java -jar /path/to/avro-tools-1.7.7.jar compile schema user.avsc .`

You now have a Java source file called `example/avro/Users.java` in your working directory.  Take a look at it now if you'd like.

Move this folder to the `AvroDemo/src/main/java` folder so we can use it in our program.

`$ mv example AvroDemo/src/main/java`

Open up Eclipse and import the Maven project `AvroDemo/pom.xml`.

From here, you can look at the code in `AvroDemo/src/main/java/com/adamjshook/demo/avro/Driver.java` for some simple usage of the Java object we generated as well as using GenericRecords from an Avro schema.

Execute the code to see the output.

Python
--------

If you have `pip`, use the below command to install Avro.  If you don't, you should go [get pip](https://pip.pypa.io/en/stable/installing/) because it is awesome.

`$ sudo pip install avro`

You can now view and execute `demo.py` to view the output.

`$ python demo.py`

Note that there is no support for code generation with Python.
