package com.browseengine.bobo.facets.data;

import java.text.DecimalFormat;
import java.util.List;

import org.apache.log4j.Logger;

public class VirtualTermIntList extends TermNumberList<Integer>
{
  private static Logger log = Logger.getLogger(VirtualTermIntList.class);

  private static int parse(String s)
  {
    if (s == null || s.length() == 0)
    {
      return 0;
    }
    else
    {
      return Integer.parseInt(s);
    }
  }

  public VirtualTermIntList(String formatString)
  {
    super(formatString);
  }

  @Override
  public boolean add(String o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  protected List<?> buildPrimitiveList(int capacity)
  {
    return null;
  }

  @Override
  public void clear()
  {
    super.clear();
  }

  @Override
  public String get(int index)
  {
    DecimalFormat formatter = _formatter.get();
    int val = (index >= 0 ? index : -1);
    if (formatter == null)
      return String.valueOf(val);
    return formatter.format(val);
  }

  public int getPrimitiveValue(int index)
  {
    if (index >= 0)
      return index;
    else
      return -1;
  }

  @Override
  public int indexOf(Object o)
  {
    int val = parse((String) o);
    return (val >= 0 ? val : -1);
  }

  public int indexOf(Integer o)
  {
    int val = o.intValue();
    return (val >= 0 ? val : -1);
  }

  public int indexOf(int val)
  {
    return (val >= 0 ? val : -1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.browseengine.bobo.facets.data.TermValueList#indexOfWithType(java.lang
   * .Object)
   */
  @Override
  public int indexOfWithType(Integer o)
  {
    int val = o.intValue();
    return (val >= 0 ? val : -1);
  }

  public int indexOfWithType(int val)
  {
    return (val >= 0 ? val : -1);
  }

  @Override
  public void seal() {}

  @Override
  protected Object parseString(String o)
  {
    return parse(o);
  }

  public boolean contains(int val)
  {
    return val >= 0;
  }
  
  @Override
  public boolean containsWithType(Integer val)
  {
    return val >= 0;
  }

  public boolean containsWithType(int val)
  {
    return val >= 0;
  }
  
  public int size()
  {
    return -1;
  }
}
