# pydis-downloader
an automated python script to download messages, attachments, and embeds from discord servers, categories, individual channels, or DM's

Current features:
- utilizes UserToken to fetch server, category, channel, or DM metadata for proper folder naming and data hiarchy. folder hiarchy is as follows: /Save_location/Server_name/Channel_name/embeds + media folders
- has a channel blacklist function for when theres less channels you dont want than there are you do want from a server
- async downloads for attachments and embeds with retry attempts, rate limiting, stalled download checking, skipping on connection timeouts, and some other under-the-hood checks to make sure downloading attachments and embeds goes smoothly.
- can download videos, images, audio, voice messages, any other file format that can uploaded to discord or uploaded via an embed, it will also sometimes download .html files of embeds and sometimes embeds dont have file extensions, this is most likely a limitation of the current embed handling of the script and will require more testing to try and fix.
- links that have already been downloaded are saved to a single file, if the link is found again it will be skipped, this is an attempt to minimize identical files, and option to disable this will be added soon.
- provides a messages.txt to save the actual chat portion of channels, deleting this will cause the entire channel to be redownloaded if needed, everything except the most recent message can be deleted if the file gets to big (1 mil messages = ~40-60MB).
- can save the raw .json output from the discord api if selected to do so, this file can become VERY large (1 mil messages as .json = ~800MB-1GB!) and is only recommened to use if you have other tools which can utilize the raw API output.
- sanitizes and truncates server, channel, DM, and file names that contain invalid characters or being longer than 255 characters to minimize the amount of issues when trying to save and view servers due to file managers having certian restrictions on what folder and file names can contain.
- some helper functions which can increase reliability and protect system hardwear, currently has a wifi connection check to ping something like google.com to see if you have internet connection, and a storage check, if the storage device being downloaded to only has X amount of GB left it will pause message fetching and file downloading if either of these checks fail.

Current limitations:
- thread channels are currently unable to be downloaded, current bandage solution: copy the channel ID of each individual thread inside a thread channel to download them as if they are normal server channels, should still grab server name to put it into the proper server folder.
- replies are not linked within message.txt yet, this is a limitation of how messages are parsed from the json data provided by the discord api, this functionality will be added in a future version.
- attachments are not listed with messages in messages.txt, again this is a limitation of how messages are parsed from the json data provided, this functionality will be added in a future version.

WARNINGS/BUGS:
- There is currently a bug that randomly happens when downloading either attachments or embeds which completely freezes any new downloads from occuring, i currently do not know why or how this happens since it only occurs once in a while but i will be doing my best to fix it, if it occurs please write an issue discribing when it occured (during attachment or embed download), how many files were remaining (terminal output should say how many were remaining), how many messages were fetched to get the attachments/embeds (if you are unable to provide this due to terminal scroll limits i understand), aprroximately how long has it been downloading for (should be able to find the time of the first download and last download but terminal scroll limits can restrict this so an estimate should suffice as well, but please use ~ when providing an estimate) 

CHANGELOG:
this code has gone through alot of changes before this repo was created so this is officaly v1 of the script
v2.1: modified some parts of the code to make them (hopefully) compatibile with windows and mac, not just linux, also removed some hardcoded file paths which ive been using for my own system, this has been replaced with customizable settings for checking stoarge limits. 
v2.0: (ig this could also be v-1.0 since im going backwards) due to a major performance decrease when having 100k+ downloads remaining i have switched the code back to its original mainly threadding setup for async downloading. 
v1.0: refactored entire script to utilize asyncio, aiohttp, and threadding for async functions like file downloading.
