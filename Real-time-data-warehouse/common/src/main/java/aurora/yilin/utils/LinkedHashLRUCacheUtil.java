package aurora.yilin.utils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/1
 */
public class LinkedHashLRUCacheUtil<K,V> extends LinkedHashMap<K,V> {

    private Long Limit;

    public LinkedHashLRUCacheUtil(Long limit) {
        super(16,0.75f,true);
        this.Limit = limit;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        if (size()>=Limit) {
            return true;
        }
        return false;
    }

    public enum LRUCacheSingle{
        LRU_CACHE(100L);

        private LinkedHashLRUCacheUtil<String,String> linkedHashLRUCache;

        LRUCacheSingle(Long limit){
            this.linkedHashLRUCache = new LinkedHashLRUCacheUtil(limit);
        }

        public LinkedHashLRUCacheUtil getLinkedHashLRUCache() {
            return linkedHashLRUCache;
        }


    }


    /**
     * 测试代码
     * @param args
     */
    public static void main(String[] args) {
        LinkedHashLRUCacheUtil linkedHashLRUCache = LinkedHashLRUCacheUtil.LRUCacheSingle.LRU_CACHE.getLinkedHashLRUCache();
        System.out.println(linkedHashLRUCache.size());
        for (int i = 0; i < 101; i++) {
            linkedHashLRUCache.put(i+"i",i);
        }
        linkedHashLRUCache.forEach((temp1,temp2) -> System.out.println(temp1.toString()+"----"+temp2));
        linkedHashLRUCache.put(0+"i",0);
        linkedHashLRUCache.forEach((temp1,temp2) -> System.out.println(temp1.toString()+"----"+temp2));
        System.out.println(linkedHashLRUCache.size());

        System.out.println(LinkedHashLRUCacheUtil.LRUCacheSingle.LRU_CACHE.getLinkedHashLRUCache().size());

    }
}
