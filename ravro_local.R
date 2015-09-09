L3 <- LETTERS[1:3]
fac <- sample(L3, 10, replace = TRUE)
d <- data.frame(x = 1, y = 1:10, fac = fac, b = rep(c(TRUE, FALSE),5), c = rep(NA, 10),
                stringsAsFactors=FALSE)
d_avro <- tempfile(fileext=".avro")
expect_true(ravro:::write.avro(d, d_avro,"d"))

d.sch <- avro_get_schema(d_avro)
expect_that(d.sch$type, equals("record"))
expect_that(d.sch$name, equals("d"))
expect_that(d.sch$fields[3][[1]]$type, equals("string"))

expect_equal(read.avro(d_avro),d)

