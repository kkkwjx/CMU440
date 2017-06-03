// Basic key value access functions to be used in server_impl

package p0

var kvstore = make(map[string][]byte)

// this function instantiates the database
func init_db() {
    kvstore = make(map[string][]byte)
}

// put inserts a new key value pair or updates the value for a
// given key in the store
func put(key string, value []byte) {
    kvstore[key] = value
}

// get fetches the value associated with the key
func get(key string) []byte {
    v, _ := kvstore[key]
    return v
}
