// Defines the interface for a KeyValueServer. DO NOT MODIFY THIS FILE!

package p0

// KeyValueServer implements a simple centralized key value database server.
type KeyValueServer interface {

    // Start starts the server on a distinct port, instantiates the database
    // and begins listening for incoming client connections. Start returns an
    // error if there was a problem listening on the specified port.
    //
    // This method should NOT block. Instead, it should spawn one or more
    // goroutines (to handle things like accepting incoming client connections,
    // broadcasting messages to clients, synchronizing access to the server's
    // set of connected clients, etc.) and then return.
    //
    // This method should return an error if the server has already been closed.
    Start(port int) error

    // Count returns the number of clients currently connected to the server.
    // This method must not be called on an un-started or closed server.
    Count() int

    // Close shuts down the server. All client connections should be closed immediately
    // and any goroutines running in the background should be signaled to return.
    Close()
}
