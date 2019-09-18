import com.etinternational.hamr.DedicatedFlow;
import com.etinternational.hamr.Flow;
import com.etinternational.hamr.KeyValueProducer;
import com.etinternational.hamr.store.kv.KeyValueContainer;
import com.etinternational.hamr.store.kv.KeyValueContainerPartition;
import com.etinternational.hamr.store.kv.KeyValueStore;
import com.etinternational.hamr.store.kv.TreeContainer;
import com.etinternational.hamr.store.kv.HashContainer;

import java.io.IOException;
import java.util.ArrayList;

public class StoreList extends KeyValueStore<String, ArrayList>{
    public StoreList(){
	super(String.class, ArrayList.class);
    }
    
    //添加一个slot：接收数据
    private final KeyValueContainer.PushSlot<String, String, String, ArrayList> putSorted = add(new KeyValueContainer.PushSlot<String, String, String, ArrayList>(){
	    @Override
	    public void accept(String key, String value, Flow flow, KeyValueContainerPartition<String, ArrayList> container) throws IOException{
		@SuppressWarnings("unchecked")
		ArrayList<String> list = (ArrayList<String>)container.get(key);
		if(list == null)
		    list = new ArrayList<>();
		list.add(value);
		container.put(key, list);
	    }
	});

    //添加一个port：发送数据
    private final KeyValueContainer.ProducerPushPort<String, ArrayList, String, String> pushSorted = add(new KeyValueContainer.ProducerPushPort<String, ArrayList, String, String>(){
	    @Override
	    public KeyValueProducer.State produce(String key, ArrayList value, DedicatedFlow<String, String> flow) throws IOException {
		@SuppressWarnings("unchecked")
		ArrayList<String> list = (ArrayList<String>) value;
		for(String aString : list)
		    flow.push(key, aString);
		return KeyValueProducer.State.CONTINUE;
	    }
	});
    
    //添加一个port：发送 list 数据
    private final KeyValueContainer.ProducerPushPort<String, ArrayList, String, ArrayList> pushList = add(new KeyValueContainer.ProducerPushPort<String, ArrayList, String, ArrayList>(){
	    @Override
	    public KeyValueProducer.State produce(String key, ArrayList value, DedicatedFlow<String, ArrayList> flow) throws IOException{
		@SuppressWarnings("unchecked")
		ArrayList<String> list = (ArrayList<String>) value;
		flow.push(key, value);
		return KeyValueProducer.State.CONTINUE;
	    }
	});

    public KeyValueContainer.PushSlot<String, String, String, ArrayList> putSorted(){
	return putSorted;
    }

    public KeyValueContainer.ProducerPushPort<String, ArrayList, String, ArrayList> pushSorted(){
	return pushList;
    }
}