package ua.bala.lectures.multiple_sources_and_sinks;

public class Transfer {

    private Transaction from;
    private Transaction to;

    public Transfer(Transaction from, Transaction to) {
        this.from = from;
        this.to = to;
    }

    public Transaction getFrom() {
        return from;
    }

    public Transaction getTo() {
        return to;
    }
}
