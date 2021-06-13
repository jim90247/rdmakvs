#pragma once

class Server {
   public:
    virtual void Listen(const char *endpoint) = 0;
};

class Client {
   public:
    virtual void Connect(const char *endpoint) = 0;
    virtual void Disconnect() = 0;
};