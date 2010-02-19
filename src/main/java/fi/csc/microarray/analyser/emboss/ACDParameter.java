package fi.csc.microarray.analyser.emboss;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.regexp.RE;
import org.emboss.jemboss.parser.AcdFunResolve;
import org.emboss.jemboss.parser.ParseAcd;

/**
 * Represents a single ACD parameter (simple, input, output, list etc.)
 * 
 * @author naktinis
 *
 */

public class ACDParameter {
    
    private String type;
    private String name;
    private String section;
    private String subsection;
    private HashMap<String, String> list;
    
    private HashMap<String, String> attributes = new HashMap<String, String>();
    
    public ACDParameter(String type, String name, String section,
                        String subsection) {
        this.setName(name);
        this.setType(type);
        this.setSection(section);
        this.setSubsection(subsection);
    }
    
    /**
     * Set parameter name.
     * 
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * Set parameter type.
     * 
     * @param type
     */
    public void setType(String type) {
        this.type = type.toLowerCase();
    }
    
    /**
     * Get parameter name that should be used in the
     * command line.
     * 
     * @return
     */
    public String getName() {
        return name;
    }
    
    /**
     * Get parameter type (string, integer, sequence etc.)
     * 
     * @return
     */
    public String getType() {
        return type;
    }
       
    /**
     * Define or update an attribute for this parameter.
     * 
     * @param name
     * @param value
     */
    public void setAttribute(String name, String value) {
        attributes.put(name, value);
    }
    
    /**
     * Find an attribute of this parameter.
     * 
     * @param name
     * @return attribute value or null if not found.
     */
    public String getAttribute(String name) {
        if (attributes.containsKey(name)) {
            return attributes.get(name);
        }
        return null;
    }
    
    /**
     * Define keys and values for a list parameter. Valid only
     * if paramter is of type "list" or "selection".
     * 
     * @param parser - Jemboss ParseAcd object.
     * @param index - parameter index in parser object.
     */
    public void setList(ParseAcd parser, Integer index) {
        list = new HashMap<String, String>();

        // Check if it is "list" or "selection"
        if (parser.getParameterAttribute(index, 0).equals("list")) {
            String[] keys = parser.getList(index);
            
            for (int i = 0; i < keys.length; i++) {
                list.put(keys[i], parser.getListLabel(index, i));
            }
        } else {
            String[] titles = parser.getSelect(index);

            for (Integer i = 0; i < parser.getSelect(index).length; i++) {
                list.put(i.toString(), titles[i]);
            }
        }

    }
    
    /**
     * Get the list HashMap for this Paramter. Valid only if
     * parameter is of type "list" of "selection".
     * 
     * @return list object.
     */
    public HashMap<String, String> getList() {
        return list;
    }
    
    /**
     * Set a section for parameter.
     * 
     * @param name
     */
    public void setSection(String name) {
        this.section = name;
    }
    
    /**
     * Get a section for parameter.
     * 
     * @return
     */
    public String getSection() {
        return section;
    }
    
    /**
     * Set a subsection for parameter.
     * 
     * @param name
     */
    public void setSubsection(String name) {
        this.subsection = name;
    }
    
    /**
     * Get a subsection for parameter.
     * 
     * @return
     */
    public String getSubsection() {
        return subsection;
    }
    
    /**
     * Determine if this parameter is required.
     * 
     * @return
     */
    public Boolean isRequired() {
        String attrStandard = getAttribute("standard");
        String attrParameter = getAttribute("parameter");
        if ((attrStandard != null && attrStandard.equals("Y")) ||
            (attrParameter != null && attrParameter.equals("Y"))) {
            return true;
        }
        return false;
    }
    
    /**
     * Determine if this parameter is optional, but should
     * be provided for a user.
     * 
     * @return
     */
    public Boolean isAdditional() {
        String attrAdditional = getAttribute("additional");
        return attrAdditional != null && attrAdditional.equals("Y");
    }
    
    /**
     * Determine if this parameter is advanced and should be normally
     * not shown to a user.
     * 
     * @return
     */
    public Boolean isAdvanced() {
        return getAttribute("standard") == null &&
               getAttribute("parameter") == null &&
               getAttribute("additional") == null;
    }
    
    /**
     * Determine if some attribute does not require additional
     * evaluation. If parameter has a value such as "$(varname)"
     * or "@($(varname)+1)" it is considered unevaluated.
     * 
     * @param attrName - attribute to be checked.
     * @return true if attribute value does not require further
     * evaluation.
     */
    public Boolean attributeIsEvaluated(String attrName) {
        String attrValue = getAttribute(attrName);
        return !(attrValue == null || attrValue.contains("$") || attrValue.contains("@"));
    }
    
    /**
     * Resolve a given ACD expression.<br><br>
     * 
     * Example:<pre>
     * resolveExp("$(variable)")
     * resolveExp("@($(sequence.length)+1)")</pre>
     * 
     * @param exp - expression to be resolved.
     * @param map - a map of variable values.
     * @return resolved value.
     */
    public static String resolveExp(String exp, LinkedHashMap<String, String> map) {
        // Simulate the map
        // TODO: store some precalculated values in map (such as acdprotein) -> server side
        // TODO: deal with calculated values (such as sequence.length) -> server side

        // Regular expression for variable names like $(variable.name)
        RE reVar = new RE("\\$\\(([\\w.]+)\\)");

        // Regular expression for operation expressions @($(bool ? $(var) : 0))
        RE reFunc = new RE("\\@\\(([\\w.+-/:?\"]+)\\)");

        // Find a variable name match
        reVar.match(exp);
        String match = reVar.getParen(1);
        String resolvedExp = exp;
        if (match != null) {
            // Find replacement string
            String substitute = "$(" + match + ")";
            if (map.containsKey(match)) {
                substitute = map.get(match);
            }

            // Find position and change
            Integer start = reVar.getParenStart(1) - 2;
            Integer end = reVar.getParenEnd(1) + 1;
            resolvedExp = exp.substring(0, start) +
                          substitute +
                          exp.substring(end, exp.length());
        }

        // Find an operation expression match
        reFunc.match(resolvedExp);
        match = reVar.getParen(1);
        if (match != null) {
            AcdFunResolve resolver = new AcdFunResolve(resolvedExp);
            resolvedExp = resolver.getResult();
        }

        if (!(exp.equals(resolvedExp))) {
            return resolveExp(resolvedExp, map);
        } else {
            // No changes were made - stop the recursion
            return resolvedExp;
        }
    }
    
    /**
     * Normalize given value according to this parameter.
     * For example, if the type is "boolean", we should
     * convert "true" to "Y" (according to ACD spec.)
     * 
     * @param value
     */
    // TODO check out what we get for ENUM, arrays etc.
    public String normalize(String value) {
        String type = getType();
        value = value.toLowerCase();
        if (type.equals("boolean")) {
            if (value.equals("yes") || value.equals("true") || value.equals("1")) {
                return "Y";
            }
            return "N";
        }
        return value;
    }
    
    /**
     * Check if a given value can be passed as this
     * parameter.
     * 
     * @param value
     */
    // TODO lists, input, output files
    public boolean validate(String value) {
        String type = getType();
        value = normalize(value);
        if (type.equals("boolean") || type.equals("toggle")) {
            
            // Boolean
            if (value == "Y" || value == "N") {
                return true;
            }
            return false;
        } else if (type.equals("integer")) {
            
            // Integer
            try {
                String attrMin = getAttribute("minimum");
                String attrMax = getAttribute("maximum");
                Integer intVal = Integer.parseInt(value);
                
                if (attrMin == null || attrMax == null || 
                    attrMin.equals("") || attrMax.equals("")) {
                    return true;
                } else { 
                    Integer minVal = Integer.parseInt(attrMin);
                    Integer maxVal = Integer.parseInt(attrMax);
                    return (intVal >= minVal && intVal <= maxVal);
                }
            }
            catch(NumberFormatException nfe) {
                return false;
            }
        } else if (type.equals("float")) {
            
            // Float
            try {
                String attrMin = getAttribute("minimum");
                String attrMax = getAttribute("maximum");
                Float floatVal = Float.parseFloat(value);
                
                if (attrMin == null || attrMax == null || 
                    attrMin.equals("") || attrMax.equals("")) {
                    return true;
                } else { 
                    Float minVal = Float.parseFloat(attrMin);
                    Float maxVal = Float.parseFloat(attrMax);
                    return (floatVal >= minVal && floatVal <= maxVal);
                }
            }
            catch(NumberFormatException nfe) {
                return false;
            }
        } else if (type.equals("array")) {
            
            // Array
            Pattern re = Pattern.compile("^(\\d+(\\.\\d+)?[ ,])*\\d+(\\.\\d+)?$");
            Matcher m = re.matcher(value);
            if (!m.find()) {
                return false;
            }
            return true;
        }
        
        return true;
    }
}
