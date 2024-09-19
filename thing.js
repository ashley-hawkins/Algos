discord_voice = require("./discord_voice.node")
discord_voice.initialize({
	audioSubsystem: "linuxPulse",
	dataDirectory: "fakeDirectory",
	logLevel: 1,
})
setTimeout(()=>{}, 1000)
discord_voice.deinitialize()
