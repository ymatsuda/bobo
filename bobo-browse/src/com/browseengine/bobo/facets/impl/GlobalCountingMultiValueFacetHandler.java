package com.browseengine.bobo.facets.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.ComparatorFactory;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetIterator;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FieldValueAccessor;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.CombinedFacetAccessible;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandler;
import com.browseengine.bobo.facets.FacetHandler.FacetDataNone;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermDoubleList;
import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermShortList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.impl.MultiValueFacetHandler.MultiValueFacetCountCollector;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.browseengine.bobo.util.IntBoundedPriorityQueue;
import com.browseengine.bobo.util.IntBoundedPriorityQueue.IntComparator;

public class GlobalCountingMultiValueFacetHandler extends RuntimeFacetHandler<FacetDataNone>
{
  private final String _baseFacetName;
  private final TermValueList<?> _valList;
  private final boolean _zeroBased;
  
  private FacetHandler<?> _baseFacetHandler;
  private int[] _count;
  
  public GlobalCountingMultiValueFacetHandler(String name, String baseFacetName, TermValueList<?> valList, boolean zeroBased)
  {
    super(name, new HashSet<String>(Arrays.asList(new String[]{baseFacetName})));
    _baseFacetName = baseFacetName;
    _valList = valList;
    _zeroBased = zeroBased;
  }
  
  public void preload(List<BoboIndexReader> readers)
  {
    int maxIdx = 0;
    // find the max value index used
    for(BoboIndexReader reader : readers)
    {
      int idx = getMaxValIndex(reader);
      if(idx > maxIdx) maxIdx = idx;
    }
    _count = new int[maxIdx];
  }
  
  private int getMaxValIndex(BoboIndexReader reader)
  {
    FacetDataCache<?> dataCache = (FacetDataCache<?>)reader.getFacetData(_baseFacetName);
    return (dataCache != null ? dataCache.maxValIndex : 0);
  }
  
  @Override
  public RandomAccessFilter buildRandomAccessFilter(String val, Properties prop) throws IOException
  {
    return _baseFacetHandler.buildRandomAccessFilter(val, prop);
  }
  
  @Override
  public RandomAccessFilter buildRandomAccessAndFilter(String[] vals, Properties prop) throws IOException
  {
    return _baseFacetHandler.buildRandomAccessAndFilter(vals, prop);
  }

  @Override
  public RandomAccessFilter buildRandomAccessOrFilter(String[] vals, Properties prop, boolean isNot) throws IOException
  {
    return _baseFacetHandler.buildRandomAccessOrFilter(vals, prop, isNot);
  }

  @Override
  public DocComparatorSource getDocComparatorSource()
  {
    return _baseFacetHandler.getDocComparatorSource();
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(final BrowseSelection sel, final FacetSpec fspec)
  {
    return new FacetCountCollectorSource()
    {
      @Override
      public FacetCountCollector getFacetCountCollector(BoboIndexReader reader, int docBase)
      {
        MultiValueFacetDataCache<?> dataCache = (MultiValueFacetDataCache<?>)_baseFacetHandler.getFacetData(reader);
        return new MultiValueFacetCountCollector(getName(), dataCache, docBase, sel, fspec, _count);
      }
    };
  }

  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id)
  {
    return _baseFacetHandler.getFieldValues(reader, id);
  }

  @Override
  public FacetDataNone load(BoboIndexReader reader) throws IOException
  {
    _baseFacetHandler = (RangeFacetHandler)getDependedFacetHandler(_baseFacetName);
    return FacetDataNone.instance;
  }
  
  @Override
  public FacetAccessible merge(FacetSpec fspec, List<FacetAccessible> facetAccList)
  {
    HashSet<GlobalCountingMultiValueFacetHandler> set = new HashSet<GlobalCountingMultiValueFacetHandler>();
    for(FacetAccessible facetAcc : facetAccList)
    {
      GlobalCountingMultiValueFacetHandler handler = ((GlobalCountingMultiValueFacetCountCollector)facetAcc)._handler;
      if(!set.contains(handler))
      {
        set.add(handler);
      }
    }
    if(set.size() == 1)
    {
      return set.iterator().next().getGlobalFacetAccesible(fspec);
    }
    ArrayList<FacetAccessible> list = new ArrayList<FacetAccessible>();
    for(GlobalCountingMultiValueFacetHandler handler : set)
    {
      list.add(handler.getGlobalFacetAccesible(fspec));
    }
    return new CombinedFacetAccessible(fspec, list);
  }
  
  private FacetAccessible getGlobalFacetAccesible(final FacetSpec fspec)
  {
    return new GlobalFacetAccessible(fspec, _valList, _count, _zeroBased);
  }
  
  private static class GlobalCountingMultiValueFacetCountCollector extends MultiValueFacetCountCollector
  {
    public GlobalCountingMultiValueFacetHandler _handler;
    
    GlobalCountingMultiValueFacetCountCollector(String name,
                                                MultiValueFacetDataCache<?> dataCache,
                                                int docBase,
                                                BrowseSelection sel,
                                                FacetSpec ospec,
                                                GlobalCountingMultiValueFacetHandler handler)
    {
      super(name, dataCache, docBase, sel, ospec, handler._count);
      _handler = handler;
    }
    
    @Override
    public int[] getCountDistribution()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public BrowseFacet getFacet(String value)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<BrowseFacet> getFacets()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public FacetIterator iterator()
    {
      throw new UnsupportedOperationException();
    }
  }
  
  private static class GlobalFacetAccessible implements FacetAccessible
  {
    private final FacetSpec _fspec;
    private final TermValueList<?> _valList;
    private final int[] _count;
    private final boolean _zeroBased;
    
    public GlobalFacetAccessible(FacetSpec fspec, TermValueList<?> valList, int[] count, boolean zeroBased)
    {
      _fspec = fspec;
      _valList = valList;
      _count = count;
      _zeroBased = zeroBased;
    }
    
    public void close()
    {
    }

    public BrowseFacet getFacet(String value)
    {
      BrowseFacet facet = null;
      int index =_valList.indexOf(value);
      if(index >= 0)
      {
        facet = new BrowseFacet(_valList.get(index), _count[index]);
      }
      else
      {
        facet = new BrowseFacet(_valList.format(value), 0);  
      }
      return facet;
    }
    
    public List<BrowseFacet> getFacets()
    {
      int start = (_zeroBased ? 0 : 1);
      if(_fspec != null)
      {
        int minCount = _fspec.getMinHitCount();
        int max = _fspec.getMaxCount();
        if(max <= 0) max = _count.length;

        List<BrowseFacet> facetColl;
        FacetSortSpec sortspec = _fspec.getOrderBy();
        if(sortspec == FacetSortSpec.OrderValueAsc)
        {
          facetColl = new ArrayList<BrowseFacet>(max);
          for(int i = start; i < _count.length; ++i) // exclude zero
          {
            int hits = _count[i];
            if(hits >= minCount)
            {
              BrowseFacet facet = new BrowseFacet(_valList.get(i), hits);
              System.out.println("DefaultFacetCountCollector: Value --> " + _valList.get(i));
              facetColl.add(facet);
            }
            if(facetColl.size() >= max) break;
          }
        }
        else //if(sortspec == FacetSortSpec.OrderHitsDesc)
        {
          ComparatorFactory comparatorFactory;
          if(sortspec == FacetSortSpec.OrderHitsDesc)
          {
            comparatorFactory = new FacetHitcountComparatorFactory();
          }
          else
          {
            comparatorFactory = _fspec.getCustomComparatorFactory();
          }

          if(comparatorFactory == null)
          {
            throw new IllegalArgumentException("facet comparator factory not specified");
          }

          final IntComparator comparator = comparatorFactory.newComparator(new FieldValueAccessor()
          {
            public String getFormatedValue(int index)
            {
              return _valList.get(index);
            }

            public Object getRawValue(int index)
            {
              return _valList.getRawValue(index);
            }
          }, _count);
          
          facetColl = new LinkedList<BrowseFacet>();
          final int forbidden = -1;
          IntBoundedPriorityQueue pq = new IntBoundedPriorityQueue(comparator, max, forbidden);

          for(int i = start; i < _count.length; ++i)
          {
            int hits = _count[i];
            if (hits >= minCount)
            {
              pq.offer(i);
            }
          }

          int val;
          while((val = pq.pollInt()) != forbidden)
          {
            BrowseFacet facet = new BrowseFacet(_valList.get(val), _count[val]);
            ((LinkedList<BrowseFacet>)facetColl).addFirst(facet);
          }
        }
        return facetColl;
      }
      else
      {
        return FacetCountCollector.EMPTY_FACET_LIST;
      }
    }

    public FacetIterator iterator()
    {
      if(_valList.getType().equals(Integer.class))
      {
        return new DefaultIntFacetIterator((TermIntList) _valList, _count, _zeroBased);
      }
      else if(_valList.getType().equals(Long.class))
      {
        return new DefaultLongFacetIterator((TermLongList) _valList, _count, _zeroBased);
      }
      else if(_valList.getType().equals(Short.class))
      {
        return new DefaultShortFacetIterator((TermShortList) _valList, _count, _zeroBased);
      }
      else if(_valList.getType().equals(Float.class))
      {
        return new DefaultFloatFacetIterator((TermFloatList) _valList, _count, _zeroBased);
      }
      else if(_valList.getType().equals(Double.class))
      {
        return new DefaultDoubleFacetIterator((TermDoubleList) _valList, _count, _zeroBased);
      }
      return new DefaultFacetIterator(_valList, _count, _zeroBased);
    } 
  }
}
