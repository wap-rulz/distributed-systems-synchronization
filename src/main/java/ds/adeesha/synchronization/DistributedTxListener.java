package ds.adeesha.synchronization;

public interface DistributedTxListener {
    void onGlobalCommit();

    void onGlobalAbort();
}