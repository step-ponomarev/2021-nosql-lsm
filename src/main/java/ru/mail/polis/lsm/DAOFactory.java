package ru.mail.polis.lsm;

import ru.mail.polis.lsm.ponomarev_stepan.InMemoryDAO;

import java.io.IOException;

public class DAOFactory {
    private DAOFactory() {}

    public static DAO create(DAOConfig config) throws IOException {
        return new InMemoryDAO();
    }
}
