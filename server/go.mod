module server

go 1.16

replace connect => ../connect

require (
	connect v0.0.0-00010101000000-000000000000
	db v0.0.0-00010101000000-000000000000
	messages v0.0.0-00010101000000-000000000000
)

replace messages => ../messages

replace db => ../db
