# Aran
Aran is an embedded key value storage based on new research paper [
Efficient Key-Value Stores with Ranged Log-Structured Merge Trees](https://ieeexplore.ieee.org/document/8457859)


# Usage 

```go
 	opts := aran.DefaultOptions()
	db, err := aran.New(opts)
	if err != nil {
		panic(err)
	}
	db.Set([]byte("hello"), []byte("schoolboy"))
	val, exist := db.Get([]byte("hello"))
	if !exist {
		panic("value not exist")
	}
	fmt.Println(string(val))
	db.Close()
```
### Note 
Don't forget to close the db, otherwise some data will be lost.

# Supported API 

- Get
- Set

# MileStones 

- Transaction API
- WAL support
- Loadbalancing small files

# Contribution

Don't think too much. just send a PR, if you need any feature or if you find any bug.

Raising an issue is also a kind of help, so feel free to raise an issue if you find any bug.

# Why another embedded KV store if badger already exist?

I was bored so I wrote this on my weekend. And, `Go` is awesome. btw I like `Rust` too.

# Acknowledgments
- Thank you sci-hub for letting me to download the paper. Language and money should not be a barrier for gaining the knowledge (My opinion)
- Thank you badger for inspiration.
- Thank you [
Efficient Key-Value Stores with Ranged Log-Structured Merge Trees's](https://ieeexplore.ieee.org/document/8457859) author for writing beautiful piece of LSM.
# About Me

I go with the name [schoolboy](https://twitter.com/hi_balaji) and I do `Go` and little bit `Rust`. If you're looking for someone to collaborate with an open source project or to fill any junior dev position. You can DM me at [@hi_balaji](https://twitter.com/hi_balaji)

# அரண் 

அரண் என்பது புதிய [ஆராய்ச்சியின்படி](https://ieeexplore.ieee.org/document/8457859)  எழுதப்பட்ட ஒரு தகவல் சேமிப்பு நிரல்.

# பயன்பாட்டு முறை 

```go
 	opts := aran.DefaultOptions()
	db, err := aran.New(opts)
	if err != nil {
		panic(err)
	}
	db.Set([]byte("வாழ்க"), []byte("மனிதாபிமானம்"))
	val, exist := db.Get([]byte("வாழ்க"))
	if !exist {
		panic("தகவல் கிடைக்கவில்லை")
	}
	fmt.Println(string(val))
	db.Close()
```
### குறிப்பு 

close அழைக்கவும், இல்லையென்றால் தகவல்களை இழக்க நேரிடும் 

# பங்குஅளிப்பாளர் குறிப்பு 

நீங்க கண்ணா மூடிக்கிட்டு கவலைபடாம PR அனுப்பலாம் 

# ஒப்புகை

- ஆய்வு கட்டுரையை பதிவு இரக்கம் செய்ய உதவிய sci-hub'கு நன்றி 
- எடுத்துக்காட்டாக இருந்த badger'கு நன்றி 
- ஆய்வு கட்டுரை எழுதிய ஆசிரியர்க்கு நன்றி [Efficient Key-Value Stores with Ranged Log-Structured Merge Trees](https://ieeexplore.ieee.org/document/8457859)

# என்னை பற்றி 
எனது பெயர் பாலாஜி ஜின்னா. நான் ஒரு பொறியியல் கல்லூரி மாணவன். உங்களுக்கு சந்தேகம் அல்லது ஒரு புதிய நட்பை உருவாக்க விரும்பினால் நீங்கள் எனது கிச்சாக [முகவரிக்கு](https://twitter.com/hi_balaji) செய்தி அனுப்பலாம் 