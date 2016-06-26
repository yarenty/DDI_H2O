val x = "1:356".split(':')


println(s" this is : ${x} ")

x(1)


"1:356".split(':')(1).toInt


val t  = "2016-01-01 00:02:30"
val tt = t.split(" ")(1).split(":")

((tt(0).toInt * 60 * 60 +tt(1).toInt*60 +tt(2).toInt ) / (10*60))+1
